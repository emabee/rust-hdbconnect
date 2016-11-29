#![feature(proc_macro)]

extern crate flexi_logger;
extern crate hdbconnect;

#[macro_use]
extern crate log;

#[macro_use]
extern crate serde_derive;

use hdbconnect::{Connection, HdbResult};

// cargo run --example masking -- --nocapture
fn main() {
    flexi_logger::init(flexi_logger::LogConfig {
                           log_to_file: false,
                           //    format: flexi_logger::opt_format,
                           //    directory: Some("z_logs".to_string()),
                           ..flexi_logger::LogConfig::new()
                       },
                       Some("info,".to_string()))
        .unwrap();

    match test_impl() {
        Err(e) => {
            error!("main() failed with {:?}", e);
            assert!(false)
        }
        Ok(_) => info!("main() ended successful"),
    }
}

fn test_impl() -> HdbResult<()> {
    let mut connection = try!(hdbconnect::Connection::new("wdfd00245307a", "30415"));
    connection.authenticate_user_password("SYSTEM", "manager").ok();

    try!(masking_1(&mut connection));

    info!("{} calls to DB were executed", connection.get_call_count());
    Ok(())
}

#[cfg_attr(rustfmt, rustfmt_skip)]
fn masking_1(connection: &mut Connection) -> HdbResult<()> {
    connection.multiple_statements_ignore_err(vec!(
        "drop view masked_view;",
        "drop function HIDE_ALL_BUT_LAST_THREE;",
    ));

    try!(connection.multiple_statements(vec!(
        "CREATE FUNCTION HIDE_ALL_BUT_LAST_THREE(input_string NVARCHAR(255)) \
            RETURNS RETVAL NVARCHAR(255) as \
        BEGIN \
            DECLARE J INTEGER; \
            DECLARE L INTEGER; \
            J := 1; \
            L := LENGTH(input_string); \
            RETVAL := ''; \
            WHILE J < L-2 DO \
                RETVAL := CONCAT(RETVAL,'*'); \
                J := J + 1; \
            END WHILE; \
            RETVAL := CONCAT (RETVAL, SUBSTRING (input_string, L-2, 3)); \
        END",

        "CREATE VIEW MASKED_VIEW \
         AS SELECT \
         OBJECT_NAME AS UNMASKED, HIDE_ALL_BUT_LAST_THREE(OBJECT_NAME) AS MASKED_OBJECT_NAME \
         FROM _SYS_REPO.ACTIVE_OBJECT;",
    )));

    let resultset = try!(connection.query_statement("select top 5 * from masked_view"));
    info!("Resultset: {}", resultset);
    Ok(())
}
