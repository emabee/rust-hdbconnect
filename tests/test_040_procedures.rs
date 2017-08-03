extern crate chrono;
extern crate flexi_logger;
extern crate hdbconnect;
#[macro_use]
extern crate log;


extern crate serde_json;

mod test_utils;

use hdbconnect::{Connection, HdbResult};

#[test] // cargo test test_procedures -- --nocapture
pub fn test_040_procedures() {
    test_utils::init_logger("test_040_procedures=info");

    match impl_test_040_procedures() {
        Err(e) => {
            error!("test_040_procedures() failed with {:?}", e);
            assert!(false)
        }
        Ok(n) => info!("{} calls to DB were executed", n),
    }
}


// Test procedures.
// Various procedures from very simple to pretty complex are tested.
fn impl_test_040_procedures() -> HdbResult<i32> {
    let mut connection = test_utils::get_authenticated_connection()?;

    very_simple_procedure(&mut connection)?;
    procedure_with_out_resultsets(&mut connection)?;
    procedure_with_secret_resultsets(&mut connection)?;

    // test IN parameters
    procedure_with_in_parameters(&mut connection)?;

    // test OUT, INOUT parameters
    procedure_with_in_and_out_parameters(&mut connection)?;

    procedure_with_in_nclob_non_consuming(&mut connection)?;
    Ok(connection.get_call_count())
}

fn very_simple_procedure(connection: &mut Connection) -> HdbResult<()> {
    info!("very_simple_procedure(): run a simple sqlscript procedure");

    test_utils::statement_ignore_err(connection, vec!["drop procedure TEST_PROCEDURE"]);
    connection.multiple_statements(vec!["\
        CREATE PROCEDURE TEST_PROCEDURE \
            LANGUAGE SQLSCRIPT SQL \
            SECURITY DEFINER \
        AS BEGIN \
            SELECT CURRENT_USER \"current user\" FROM DUMMY; \
        END"])?;

    let mut response = connection.any_statement("call TEST_PROCEDURE")?;
    response.get_success()?;
    let _resultset = response.get_resultset()?;
    test_utils::statement_ignore_err(connection, vec!["drop procedure TEST_PROCEDURE"]);
    Ok(())
}

fn procedure_with_out_resultsets(connection: &mut Connection) -> HdbResult<()> {
    info!("procedure_with_out_resultsets(): run a sqlscript procedure with resultsets as OUT \
           parameters");

    test_utils::statement_ignore_err(connection, vec!["drop procedure GET_PROCEDURES"]);
    connection.multiple_statements(vec!["\
    CREATE PROCEDURE GET_PROCEDURES( \
        OUT table1 TABLE(schema_name NVARCHAR(256), procedure_name NVARCHAR(256) ), \
        OUT table2 TABLE(sql_security NVARCHAR(256), DEFAULT_SCHEMA_NAME NVARCHAR(256) ),  \
        OUT table3 TABLE(PROCEDURE_TYPE NVARCHAR(256), OWNER_NAME NVARCHAR(256) ) \
    ) AS BEGIN \
        table1 =    SELECT schema_name, procedure_name \
                    FROM PROCEDURES \
                    WHERE IS_VALID = 'TRUE'; \
        \
        table2 =    SELECT sql_security, DEFAULT_SCHEMA_NAME  \
                    FROM PROCEDURES \
                    WHERE IS_VALID = 'TRUE'; \
        table3 =    SELECT PROCEDURE_TYPE, OWNER_NAME  \
                    FROM PROCEDURES \
                    WHERE IS_VALID = 'TRUE'; \
    END;"])?;

    let mut response = connection.any_statement("call GET_PROCEDURES(?,?,?)")?;
    response.get_success()?;
    let l1 = response.get_resultset()?.len()?;
    let l2 = response.get_resultset()?.len()?;
    let l3 = response.get_resultset()?.len()?;
    assert_eq!(l1, l2);
    assert_eq!(l1, l3);
    Ok(())
}

fn procedure_with_secret_resultsets(connection: &mut Connection) -> HdbResult<()> {
    info!("procedure_with_secret_resultsets(): run a sqlscript procedure with implicit resultsets");

    test_utils::statement_ignore_err(connection, vec!["drop procedure GET_PROCEDURES_SECRETLY"]);
    connection.multiple_statements(vec!["\
        CREATE PROCEDURE GET_PROCEDURES_SECRETLY() \
            AS BEGIN \
                SELECT schema_name, procedure_name \
                FROM PROCEDURES \
                WHERE IS_VALID = 'TRUE'; \
                \
                SELECT sql_security, DEFAULT_SCHEMA_NAME  \
                FROM PROCEDURES \
                WHERE IS_VALID = 'TRUE'; \
                \
                SELECT PROCEDURE_TYPE, OWNER_NAME  \
                FROM PROCEDURES \
                WHERE IS_VALID = 'TRUE'; \
            END;"])?;

    let mut response = connection.any_statement("call GET_PROCEDURES_SECRETLY()")?;

    response.get_success()?;
    let l1 = response.get_resultset()?.len()?;
    let l2 = response.get_resultset()?.len()?;
    let l3 = response.get_resultset()?.len()?;
    assert_eq!(l1, l2);
    assert_eq!(l1, l3);
    Ok(())
}

fn procedure_with_in_parameters(connection: &mut Connection) -> HdbResult<()> {
    info!("procedure_with_in_parameters(): run a sqlscript procedure with input parameters");

    test_utils::statement_ignore_err(connection, vec!["drop procedure TEST_INPUT_PARS"]);
    connection.multiple_statements(vec!["\
        CREATE  PROCEDURE TEST_INPUT_PARS(IN some_number INT, IN some_string NVARCHAR(20)) \
          AS BEGIN \
            SELECT some_number AS \"I\", some_string AS \"A\" FROM DUMMY;\
          END;"])?;

    let mut prepared_stmt = connection.prepare("call TEST_INPUT_PARS(?,?)")?;
    prepared_stmt.add_batch(&(42, "is between 41 and 43"))?;
    let mut response = prepared_stmt.execute_batch()?;
    response.get_success()?;
    let mut rs = response.get_resultset()?;
    let row = rs.pop_row().unwrap();
    assert_eq!(row.values.get(0).unwrap().get_i32()?, 42);
    assert_eq!(row.values.get(1).unwrap().get_string()?, "is between 41 and 43");
    Ok(())
}


fn procedure_with_in_and_out_parameters(connection: &mut Connection) -> HdbResult<()> {
    info!("procedure_with_in_and_out_parameters(): verify that we can run a sqlscript procedure \
           with input and output parameters");

    test_utils::statement_ignore_err(connection, vec!["drop procedure TEST_INPUT_AND_OUTPUT_PARS"]);
    connection.multiple_statements(vec!["\
    CREATE  PROCEDURE \
        TEST_INPUT_AND_OUTPUT_PARS( \
            IN some_number INT, \
            OUT some_string NVARCHAR(40) \
        ) AS \
    BEGIN \
        some_string = 'my first output parameter'; \
        SELECT some_number AS \"I\" FROM DUMMY; \
    END;"])?;

    let mut prepared_stmt = connection.prepare("call TEST_INPUT_AND_OUTPUT_PARS(?,?)")?;
    prepared_stmt.add_batch(&42)?;

    let mut response = prepared_stmt.execute_batch()?;
    response.get_success()?;
    let op = response.get_output_parameters()?;
    assert_eq!(op.values.get(0).unwrap().get_string()?, "my first output parameter");

    let mut rs = response.get_resultset()?;
    let row = rs.pop_row().unwrap();
    assert_eq!(row.values.get(0).unwrap().get_i32()?, 42);

    Ok(())
}

fn procedure_with_in_nclob_non_consuming(connection: &mut Connection) -> HdbResult<()> {
    info!("procedure_with_in_nclob_non_consuming(): convert input parameter to nclob");

    test_utils::statement_ignore_err(connection, vec!["drop procedure TEST_CLOB_INPUT_PARS"]);
    connection.multiple_statements(vec!["\
    CREATE PROCEDURE \
        TEST_CLOB_INPUT_PARS(IN some_string NCLOB) \
    AS BEGIN \
        SELECT some_string AS \"A\" FROM DUMMY; \
    END;"])?;

    let mut prepared_stmt = connection.prepare("call TEST_CLOB_INPUT_PARS(?)")?;
    let my_parameter = "nclob string".to_string();
    prepared_stmt.add_batch(&my_parameter)?;
    debug!("Still owned {:?}", &my_parameter);
    let mut response = prepared_stmt.execute_batch()?;
    response.get_success()?;
    let mut rs = response.get_resultset()?;
    let row = rs.pop_row().unwrap();
    assert_eq!(row.values.get(0).unwrap().get_string()?, "nclob string");

    Ok(())
}
