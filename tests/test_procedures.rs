#![feature(custom_derive, plugin)]
#![plugin(serde_macros)]

extern crate chrono;
#[macro_use]
extern crate log;
extern crate flexi_logger;
extern crate hdbconnect;
extern crate serde;

use hdbconnect::{Connection,DbcResult,test_utils};

// cargo test test_procedures -- --nocapture
#[test]
pub fn test_procedures() {
    use flexi_logger::LogConfig;
    // hdbconnect::protocol::lowlevel::resultset=debug,\
    flexi_logger::init(LogConfig {
            log_to_file: true,
            .. LogConfig::new() },
            Some("debug,\
            ".to_string())).unwrap();


    match impl_test_procedures() {
        Err(e) => {error!("test_procedures() failed with {:?}",e); assert!(false)},
        Ok(n) => {info!("{} calls to DB were executed", n)},
    }
}

fn impl_test_procedures() -> DbcResult<i32> {
    let mut connection = try!(hdbconnect::Connection::new("wdfd00245307a", "30415"));
    connection.authenticate_user_password("SYSTEM", "manager").ok();

    try!(very_simple_procedure(&mut connection));
    try!(procedure_with_out_resultsets(&mut connection));
    try!(procedure_with_secret_resultsets(&mut connection));

    // test IN, OUT, INOUT parameters

    Ok(connection.get_call_count())
}

fn very_simple_procedure(connection: &mut Connection) -> DbcResult<()> {
    info!("verify that we can run a simple sqlscript procedure");

    test_utils::statement_ignore_err(connection, vec!("drop procedure TEST_PROCEDURE"));
    try!(test_utils::multiple_statements(connection, vec!(
        "CREATE PROCEDURE TEST_PROCEDURE \
            LANGUAGE SQLSCRIPT \
            SQL SECURITY DEFINER AS \
            BEGIN \
                SELECT CURRENT_USER \"current user\" FROM DUMMY;\
            END",
    )));

    let mut response = try!(connection.any_statement("call TEST_PROCEDURE"));
    debug!("response: {:?}",response);
    try!(response.get_success());
    let resultset = try!(response.get_resultset());
    debug!("resultset = {}",resultset);
    Ok(())
}

fn procedure_with_out_resultsets(connection: &mut Connection) -> DbcResult<()> {
    info!("verify that we can run a sqlscript procedure with resultsets as OUT parameters");

    test_utils::statement_ignore_err(connection, vec!("drop procedure GET_PROCEDURES"));
    try!(test_utils::multiple_statements(connection, vec!("\
        CREATE  PROCEDURE GET_PROCEDURES( \
            OUT procedures TABLE(schema_name NVARCHAR(256), name NVARCHAR(256)) ,\
            OUT hana_dus TABLE(du NVARCHAR(256), vendor NVARCHAR(256)), \
            OUT other_dus TABLE(du NVARCHAR(256), vendor NVARCHAR(256)) \
        ) AS \
        BEGIN \
            procedures = SELECT schema_name AS schema_name, \
                                procedure_name AS name \
                         FROM PROCEDURES \
                         WHERE IS_VALID = 'TRUE'; \
\
            hana_dus = select delivery_unit as du, vendor \
                       from _SYS_REPO.DELIVERY_UNITS where delivery_unit like 'HANA%'; \
            other_dus = select delivery_unit as du, vendor \
                       from _SYS_REPO.DELIVERY_UNITS where not delivery_unit like 'HANA%'; \
        END;",
    )));

    let mut response = try!(connection.any_statement("call GET_PROCEDURES(?,?,?)"));
    debug!("response = {:?}", response);

    try!(response.get_success());
    debug!("procedures = {}",try!(response.get_resultset()));
    debug!("hana_dus = {}",try!(response.get_resultset()));
    debug!("other_dus = {}",try!(response.get_resultset()));

    Ok(())
}

fn procedure_with_secret_resultsets(connection: &mut Connection) -> DbcResult<()> {
    info!("verify that we can run a sqlscript procedure with implicit resultsets");

    test_utils::statement_ignore_err(connection, vec!("drop procedure GET_PROCEDURES_SECRETLY"));
    try!(test_utils::multiple_statements(connection, vec!("\
        CREATE  PROCEDURE GET_PROCEDURES_SECRETLY() AS \
        BEGIN \
            SELECT  schema_name AS schema_name, \
                    procedure_name AS name \
            FROM PROCEDURES \
            WHERE IS_VALID = 'TRUE'; \
\
            SELECT  delivery_unit as du, vendor \
            FROM _SYS_REPO.DELIVERY_UNITS \
            WHERE delivery_unit like 'HANA%'; \
\
            SELECT  delivery_unit as du, vendor \
            FROM _SYS_REPO.DELIVERY_UNITS \
            WHERE not delivery_unit like 'HANA%'; \
        END;",
    )));

    let mut response = try!(connection.any_statement("call GET_PROCEDURES_SECRETLY()"));
    debug!("response = {:?}", response);

    try!(response.get_success());
    debug!("procedures = {}",try!(response.get_resultset()));
    debug!("hana_dus = {}",try!(response.get_resultset()));
    debug!("other_dus = {}",try!(response.get_resultset()));

    Ok(())
}
