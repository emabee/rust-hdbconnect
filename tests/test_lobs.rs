#![feature(proc_macro)]

extern crate chrono;
extern crate hdbconnect;

#[macro_use]
extern crate log;

extern crate serde;

#[macro_use]
extern crate serde_derive;

mod test_utils;

use hdbconnect::{Connection, HdbResult, HdbValue};

use serde::bytes::{ByteBuf, Bytes};

// cargo test test_lobs -- --nocapture
#[test]
pub fn test_lobs() {
    test_utils::init_logger(false, "info,test_lobs=debug,hdbconnect::rs_serde::ser=trace");

    match impl_test_lobs() {
        Err(e) => {
            error!("test_lobs() failed with {:?}", e);
            assert!(false)
        }
        Ok(n) => info!("{} calls to DB were executed", n),
    }
}

fn impl_test_lobs() -> HdbResult<i32> {
    let mut connection = test_utils::get_authenticated_connection();
    try!(test_read_blob(&mut connection));
    try!(test_write_blob(&mut connection));
    Ok(connection.get_call_count())
}


fn test_read_blob(connection: &mut Connection) -> HdbResult<()> {
    info!("select a single table line with a lob, and fetch the complete lob using the default \
           huge lob read length; select it again with a small lob read length (1kB), and compare \
           the results");


    #[derive(Serialize, Deserialize, Debug)]
    struct ActiveObject {
        package_id: String,
        object_name: String,
        object_suffix: String,
        edit: u8,
        bdata_length: usize,
        bdata: ByteBuf,
        object_status: u8,
    }

    let stmt = "select  PACKAGE_ID as \"package_id\", OBJECT_NAME as \"object_name\", \
                OBJECT_SUFFIX as \"object_suffix\", EDIT as \"edit\", LENGTH(BDATA) as \
                \"bdata_length\", BDATA as \"bdata\", OBJECT_STATUS as \"object_status\" from    \
                _SYS_REPO.ACTIVE_OBJECT where   package_id = 'sap.ui5.1.sdk.docs.guide' and     \
                object_name = 'loiof144853312cd42a1bff62ce4695eba2d_LowRes' and     object_suffix \
                = 'png' ";

    let resultset = try!(connection.query_statement(stmt));
    debug!("ResultSet: {:?}", resultset);

    let first: ActiveObject = try!(resultset.into_typed());
    debug!("Typed Result: {:?}", first);

    assert_eq!(first.bdata_length, first.bdata.len());

    connection.set_lob_read_length(1024);
    let resultset = try!(connection.query_statement(stmt));
    let second: ActiveObject = try!(resultset.into_typed());
    assert_eq!(second.bdata_length, second.bdata.len());

    assert_eq!(first.bdata, second.bdata);
    Ok(())
}

fn test_write_blob(connection: &mut Connection) -> HdbResult<()> {
    info!("write a line with a lob to DB, and read it again");

    #[derive(Deserialize,Debug)]
    struct TestWriteLob {
        f1: String,
        fblob: ByteBuf,
        f3: i32,
    }

    test_utils::statement_ignore_err(connection, vec!["drop table TEST_WRITE_BLOB"]);
    try!(test_utils::multiple_statements(connection,
                                         vec!["create table TEST_WRITE_BLOB (\"f1\" \
                                               NVARCHAR(10), \"fblob\" BLOB, \"f3\" INT)"]));

    let stmt = "select  BDATA as \"bdata\" \
                from    _SYS_REPO.ACTIVE_OBJECT \
                where   package_id = 'sap.ui5.1.sdk.docs.guide' \
                and     object_name = 'loiof144853312cd42a1bff62ce4695eba2d_LowRes'
                and     object_suffix = 'png' ";
    let mut resultset = try!(connection.query_statement(stmt));
    let bytes: &Vec<u8> = {
        if let &mut HdbValue::N_BLOB(Some(ref mut blob)) = resultset.get_value(0, 0).unwrap() {
            try!(blob.ref_bytes())
        } else {
            panic!("didn't get the blob data");
        }
    };

    let insert_stmt_str = "insert into TEST_WRITE_BLOB (\"f1\", \"fblob\", \"f3\") values(?, ?, ?)";
    let mut insert_stmt = try!(connection.prepare(insert_stmt_str));
    let data = ("TEST", Bytes::from(bytes), 42_i32);
    trace!("data = {:?}", data);
    try!(insert_stmt.add_batch(&data));
    try!(insert_stmt.execute_batch());

    connection.set_lob_read_length(1_000_000);
    let stmt = "select * from TEST_WRITE_BLOB";
    let mut twl: TestWriteLob = try!(try!(connection.query_statement(stmt)).into_typed());

    assert_eq!(*bytes, twl.fblob.as_mut());
    Ok(())
}
