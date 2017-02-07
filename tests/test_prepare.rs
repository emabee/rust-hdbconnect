extern crate chrono;
extern crate hdbconnect;
extern crate flexi_logger;

#[macro_use]
extern crate log;

#[macro_use]
extern crate serde_derive;
extern crate serde_json;

mod test_utils;

use hdbconnect::Connection;
use hdbconnect::HdbResult;


#[test] // cargo test test_prepare -- --nocapture
pub fn test_prepare() {
    test_utils::init_logger(false, "info");

    match impl_test_prepare() {
        Err(e) => {
            error!("test_prepare() failed with {:?}", e);
            assert!(false)
        }
        Ok(i) => info!("{} calls to DB were executed", i),
    }
}

fn impl_test_prepare() -> HdbResult<i32> {
    let mut connection = test_utils::get_authenticated_connection();

    try!(prepare_insert_statement(&mut connection));

    Ok(connection.get_call_count())
}

fn prepare_insert_statement(connection: &mut Connection) -> HdbResult<()> {
    info!("test statement preparation and transactional correctness (auto_commit on/off, \
           rollbacks)");
    test_utils::statement_ignore_err(connection, vec!["drop table TEST_PREPARE"]);
    try!(test_utils::multiple_statements(connection,
                                         vec!["create table TEST_PREPARE (F_S NVARCHAR(20), \
                                               F_I INT)"]));

    #[allow(non_snake_case)]
    #[derive(Deserialize, Debug)]
    struct TestStruct {
        F_S: Option<String>,
        F_I: Option<i32>,
    }

    let insert_stmt_str = "insert into TEST_PREPARE (F_S, F_I) values(?, ?)";

    // prepare & execute
    let mut insert_stmt = try!(connection.prepare(insert_stmt_str));
    try!(insert_stmt.add_batch(&("conn1-auto1", 45_i32)));
    try!(insert_stmt.add_batch(&("conn1-auto2", 46_i32)));
    try!(insert_stmt.execute_batch());

    // prepare & execute on second connection
    let connection2 = try!(connection.spawn());
    let mut insert_stmt2 = try!(connection2.prepare(insert_stmt_str));
    try!(insert_stmt2.add_batch(&("conn2-auto1", 45_i32)));
    try!(insert_stmt2.add_batch(&("conn2-auto2", 46_i32)));
    try!(insert_stmt2.execute_batch());

    // prepare & execute on first connection with auto_commit off,
    // rollback, do it again and commit
    connection.set_auto_commit(false);
    let mut insert_stmt = try!(connection.prepare(insert_stmt_str));
    try!(insert_stmt.add_batch(&("conn1-rollback1", 45_i32)));
    try!(insert_stmt.add_batch(&("conn1-rollback2", 46_i32)));
    try!(insert_stmt.add_batch(&("conn1-rollback3", 47_i32)));
    try!(insert_stmt.execute_batch());
    try!(connection.rollback());

    try!(insert_stmt.add_batch(&("conn1-commit1", 45_i32)));
    try!(insert_stmt.add_batch(&("conn1-commit2", 46_i32)));
    try!(insert_stmt.execute_batch());
    try!(connection.commit());


    // prepare, execute batch, rollback in new spawn
    let mut connection3 = try!(connection.spawn());
    let mut insert_stmt3 = try!(connection3.prepare(insert_stmt_str));
    try!(insert_stmt3.add_batch(&("conn3-auto1", 45_i32)));
    try!(insert_stmt3.add_batch(&("conn3-auto2", 46_i32)));
    try!(insert_stmt3.add_batch(&("conn3-auto3", 47_i32)));
    try!(insert_stmt3.execute_batch());
    try!(connection3.rollback());


    let resultset = try!(connection.query_statement("select * from TEST_PREPARE"));
    let typed_result: Vec<TestStruct> = try!(resultset.into_typed());

    debug!("Typed Result: {:?}", typed_result);
    assert_eq!(typed_result.len(), 6);
    Ok(())
}
