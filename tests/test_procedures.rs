extern crate chrono;
extern crate hdbconnect;
extern crate flexi_logger;

#[macro_use]
extern crate log;

#[macro_use]
extern crate serde_derive;

extern crate serde_json;

mod test_utils;

use hdbconnect::{Connection, HdbResult};

#[test] // cargo test test_procedures -- --nocapture
pub fn test_procedures() {
    test_utils::init_logger(false, "info");

    match impl_test_procedures() {
        Err(e) => {
            error!("test_procedures() failed with {:?}", e);
            assert!(false)
        }
        Ok(n) => info!("{} calls to DB were executed", n),
    }
}

fn impl_test_procedures() -> HdbResult<i32> {
    let mut connection = test_utils::get_authenticated_connection();

    try!(very_simple_procedure(&mut connection));
    try!(procedure_with_out_resultsets(&mut connection));
    try!(procedure_with_secret_resultsets(&mut connection));

    // test IN parameters
    try!(procedure_with_in_parameters(&mut connection));

    // test OUT, INOUT parameters
    try!(procedure_with_in_and_out_parameters(&mut connection));

    Ok(connection.get_call_count())
}

fn very_simple_procedure(connection: &mut Connection) -> HdbResult<()> {
    info!("verify that we can run a simple sqlscript procedure");

    test_utils::statement_ignore_err(connection, vec!["drop procedure TEST_PROCEDURE"]);
    try!(test_utils::multiple_statements(connection,
                                         vec!["CREATE PROCEDURE TEST_PROCEDURE LANGUAGE \
                                               SQLSCRIPT SQL SECURITY DEFINER AS BEGIN SELECT \
                                               CURRENT_USER \"current user\" FROM DUMMY;END"]));

    let mut response = try!(connection.any_statement("call TEST_PROCEDURE"));
    debug!("response: {:?}", response);
    try!(response.get_success());
    let resultset = try!(response.get_resultset());
    debug!("resultset = {}", resultset);
    Ok(())
}

fn procedure_with_out_resultsets(connection: &mut Connection) -> HdbResult<()> {
    info!("verify that we can run a sqlscript procedure with resultsets as OUT parameters");

    test_utils::statement_ignore_err(connection, vec!["drop procedure GET_PROCEDURES"]);
    try!(test_utils::multiple_statements(connection,
                                         vec!["\
        CREATE  PROCEDURE GET_PROCEDURES( OUT \
                                               procedures TABLE(schema_name NVARCHAR(256), \
                                               name NVARCHAR(256)) ,OUT hana_dus TABLE(du \
                                               NVARCHAR(256), vendor NVARCHAR(256)), OUT \
                                               other_dus TABLE(du NVARCHAR(256), vendor \
                                               NVARCHAR(256)) ) AS BEGIN procedures = SELECT \
                                               schema_name AS schema_name, procedure_name AS \
                                               name FROM PROCEDURES WHERE IS_VALID = 'TRUE'; \
            \
                                               hana_dus = select delivery_unit as du, vendor \
                                               from _SYS_REPO.DELIVERY_UNITS where \
                                               delivery_unit like 'HANA%'; other_dus = select \
                                               delivery_unit as du, vendor from \
                                               _SYS_REPO.DELIVERY_UNITS where not \
                                               delivery_unit like 'HANA%'; END;"]));

    let mut response = try!(connection.any_statement("call GET_PROCEDURES(?,?,?)"));
    debug!("response = {:?}", response);

    try!(response.get_success());
    debug!("procedures = {}", try!(response.get_resultset()));
    debug!("hana_dus = {}", try!(response.get_resultset()));
    debug!("other_dus = {}", try!(response.get_resultset()));

    Ok(())
}

fn procedure_with_secret_resultsets(connection: &mut Connection) -> HdbResult<()> {
    info!("verify that we can run a sqlscript procedure with implicit resultsets");

    test_utils::statement_ignore_err(connection, vec!["drop procedure GET_PROCEDURES_SECRETLY"]);
    try!(test_utils::multiple_statements(connection,
                                         vec!["\
        CREATE  PROCEDURE \
                                               GET_PROCEDURES_SECRETLY() AS BEGIN SELECT  \
                                               schema_name AS schema_name, procedure_name AS \
                                               name FROM PROCEDURES WHERE IS_VALID = 'TRUE'; \
            \
                                               SELECT  delivery_unit as du, vendor FROM \
                                               _SYS_REPO.DELIVERY_UNITS WHERE delivery_unit \
                                               like 'HANA%'; \
            SELECT  \
                                               delivery_unit as du, vendor FROM \
                                               _SYS_REPO.DELIVERY_UNITS WHERE not \
                                               delivery_unit like 'HANA%'; END;"]));

    let mut response = try!(connection.any_statement("call GET_PROCEDURES_SECRETLY()"));
    debug!("response = {:?}", response);

    try!(response.get_success());
    debug!("procedures = {}", try!(response.get_resultset()));
    debug!("hana_dus = {}", try!(response.get_resultset()));
    debug!("other_dus = {}", try!(response.get_resultset()));

    Ok(())
}

fn procedure_with_in_parameters(connection: &mut Connection) -> HdbResult<()> {
    info!("verify that we can run a sqlscript procedure with input parameters");

    test_utils::statement_ignore_err(connection, vec!["drop procedure TEST_INPUT_PARS"]);
    try!(test_utils::multiple_statements(connection,
                                         vec!["\
        CREATE  PROCEDURE TEST_INPUT_PARS(IN \
                                               some_number INT, IN some_string NVARCHAR(20)) \
                                               AS BEGIN SELECT some_number AS \"I\", \
                                               some_string AS \"A\" FROM DUMMY; END;"]));

    let mut prepared_stmt = try!(connection.prepare("call TEST_INPUT_PARS(?,?)"));
    try!(prepared_stmt.add_batch(&(42, "is between 41 and 43")));
    let mut response = try!(prepared_stmt.execute_batch());
    debug!("response = {:?}", response);
    try!(response.get_success());
    let rs = try!(response.get_resultset());
    debug!("resultset = {:?}", rs);
    Ok(())
}


fn procedure_with_in_and_out_parameters(connection: &mut Connection) -> HdbResult<()> {
    info!("verify that we can run a sqlscript procedure with input and output parameters");

    test_utils::statement_ignore_err(connection, vec!["drop procedure TEST_INPUT_AND_OUTPUT_PARS"]);
    try!(test_utils::multiple_statements(connection,
                                         vec!["\
        CREATE  PROCEDURE \
                                               TEST_INPUT_AND_OUTPUT_PARS(IN some_number INT, \
                                               OUT some_string NVARCHAR(40)) AS BEGIN \
                                               some_string = 'my first output parameter';
            \
                                               SELECT some_number AS \"I\" FROM DUMMY; END;"]));

    let mut prepared_stmt = try!(connection.prepare("call TEST_INPUT_AND_OUTPUT_PARS(?,?)"));
    try!(prepared_stmt.add_batch(&(42)));
    let mut response = try!(prepared_stmt.execute_batch());
    debug!("response = {:?}", response);
    try!(response.get_success());
    let op = try!(response.get_output_parameters());
    debug!("output_parameters = {}", op);
    let rs = try!(response.get_resultset());
    debug!("resultset = {}", rs);
    Ok(())
}
