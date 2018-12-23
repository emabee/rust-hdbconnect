mod test_utils;

use flexi_logger::ReconfigurationHandle;
use hdbconnect::{
    BaseTypeId, Connection, HdbResult, ParameterBinding, ParameterDirection, ResultSet, Row,
};
use log::{debug, info};

// Test various procedures, from very simple to pretty complex
#[test]
pub fn test_040_procedures() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger("info");
    let mut connection = test_utils::get_authenticated_connection()?;

    very_simple_procedure(&mut log_handle, &mut connection)?;
    procedure_with_out_resultsets(&mut log_handle, &mut connection)?;
    procedure_with_secret_resultsets(&mut log_handle, &mut connection)?;
    procedure_with_in_parameters(&mut log_handle, &mut connection)?;
    procedure_with_in_and_out_parameters(&mut log_handle, &mut connection)?;
    procedure_with_in_nclob_non_consuming(&mut log_handle, &mut connection)?;

    info!("{} calls to DB were executed", connection.get_call_count()?);
    Ok(())
}

fn very_simple_procedure(
    _log_handle: &mut ReconfigurationHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    info!("very_simple_procedure(): run a simple sqlscript procedure");

    connection.multiple_statements_ignore_err(vec!["drop procedure TEST_PROCEDURE"]);
    connection.multiple_statements(vec![
        "\
         CREATE PROCEDURE TEST_PROCEDURE \
         LANGUAGE SQLSCRIPT SQL SECURITY DEFINER \
         AS BEGIN \
         SELECT CURRENT_USER \"current user\" FROM DUMMY; \
         END",
    ])?;

    let mut response = connection.statement("call TEST_PROCEDURE")?;
    response.get_success()?;
    let _resultset = response.get_resultset()?;
    connection.multiple_statements_ignore_err(vec!["drop procedure TEST_PROCEDURE"]);
    Ok(())
}

fn procedure_with_out_resultsets(
    _log_handle: &mut ReconfigurationHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    info!(
        "procedure_with_out_resultsets(): run a sqlscript procedure with resultsets as OUT \
         parameters"
    );

    connection.multiple_statements_ignore_err(vec!["drop procedure GET_PROCEDURES"]);
    connection.multiple_statements(vec![
        "\
         CREATE PROCEDURE \
         GET_PROCEDURES( \
         OUT table1 TABLE(schema_name NVARCHAR(256), procedure_name NVARCHAR(256) ), \
         OUT table2 TABLE(SEKURITATE NVARCHAR(256), DEFAULT_SCHEMA_NAME NVARCHAR(256) ) \
         ) \
         AS BEGIN \
         table1 = SELECT schema_name, procedure_name \
         FROM PROCEDURES \
         WHERE IS_VALID = 'TRUE'; \
         table2 = SELECT sql_security as SEKURITATE, DEFAULT_SCHEMA_NAME \
         FROM PROCEDURES \
         WHERE IS_VALID = 'TRUE' \
         UNION ALL \
         SELECT sql_security as SEKURITATE, DEFAULT_SCHEMA_NAME \
         FROM PROCEDURES \
         WHERE IS_VALID = 'TRUE'; \
         END;",
    ])?;

    let mut response = connection.statement("call GET_PROCEDURES(?,?)")?;
    response.get_success()?;
    let l1 = response.get_resultset()?.total_number_of_rows()?;
    let l2 = response.get_resultset()?.total_number_of_rows()?;
    assert_eq!(2 * l1, l2);
    Ok(())
}

fn procedure_with_secret_resultsets(
    _log_handle: &mut ReconfigurationHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    info!("procedure_with_secret_resultsets(): run a sqlscript procedure with implicit resultsets");

    connection.multiple_statements_ignore_err(vec!["drop procedure GET_PROCEDURES_SECRETLY"]);
    connection.multiple_statements(vec![
        "\
         CREATE PROCEDURE \
         GET_PROCEDURES_SECRETLY() \
         AS BEGIN \
         SELECT schema_name, procedure_name \
         FROM PROCEDURES \
         WHERE IS_VALID = 'TRUE'; \
         (SELECT sql_security as SEKURITATE, DEFAULT_SCHEMA_NAME \
         FROM PROCEDURES \
         WHERE IS_VALID = 'TRUE') \
         UNION ALL \
         (SELECT sql_security as SEKURITATE, DEFAULT_SCHEMA_NAME \
         FROM PROCEDURES \
         WHERE IS_VALID = 'TRUE'); \
         END;",
    ])?;

    let mut response = connection.statement("call GET_PROCEDURES_SECRETLY()")?;

    response.get_success()?;
    let l1 = response.get_resultset()?.total_number_of_rows()?;
    let l2 = response.get_resultset()?.total_number_of_rows()?;
    assert_eq!(2 * l1, l2);
    Ok(())
}

fn procedure_with_in_parameters(
    _log_handle: &mut ReconfigurationHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    info!("procedure_with_in_parameters(): run a sqlscript procedure with input parameters");

    connection.multiple_statements_ignore_err(vec!["drop procedure TEST_INPUT_PARS"]);
    connection.multiple_statements(vec![
        "\
         CREATE PROCEDURE \
         TEST_INPUT_PARS( \
         IN some_number INT, \
         IN some_string NVARCHAR(20)) \
         AS BEGIN \
         SELECT some_number AS \"I\", some_string AS \"A\" FROM DUMMY; \
         END;",
    ])?;

    let mut prepared_stmt = connection.prepare("call TEST_INPUT_PARS(?,?)")?;

    prepared_stmt.add_batch(&(42, "is between 41 and 43"))?;
    let mut response = prepared_stmt.execute_batch()?;
    response.get_success()?;
    let mut rs: ResultSet = response.get_resultset()?;
    let mut row: Row = rs.pop_row().unwrap();
    let value: i32 = row.field_into(0)?;
    assert_eq!(value, 42_i32);
    let value: String = row.field_into(1)?;
    assert_eq!(&value, "is between 41 and 43");
    Ok(())
}

fn procedure_with_in_and_out_parameters(
    _log_handle: &mut ReconfigurationHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    info!(
        "procedure_with_in_and_out_parameters(): verify that we can run a sqlscript procedure \
         with input and output parameters"
    );

    connection.multiple_statements_ignore_err(vec!["drop procedure TEST_INPUT_AND_OUTPUT_PARS"]);
    connection.multiple_statements(vec![
        "CREATE  PROCEDURE \
         TEST_INPUT_AND_OUTPUT_PARS( \
         IN some_number INT, OUT some_string NVARCHAR(40) ) \
         AS BEGIN \
         some_string = 'some output parameter'; \
         SELECT some_number AS \"I\" FROM DUMMY;\
         END;",
    ])?;

    let mut prepared_stmt = connection.prepare("call TEST_INPUT_AND_OUTPUT_PARS(?,?)")?;
    prepared_stmt.add_batch(&42)?;

    let mut response = prepared_stmt.execute_batch()?;
    response.get_success()?;
    let mut op = response.get_output_parameters()?;
    {
        let par_desc = op.parameter_descriptor(0)?;
        assert_eq!(par_desc.binding(), ParameterBinding::Optional);
        assert_eq!(par_desc.type_id().base_type_id(), &BaseTypeId::NVARCHAR);
        assert_eq!(par_desc.direction(), ParameterDirection::OUT);
        assert_eq!(par_desc.name(), Some(&"SOME_STRING".to_string()));
    }
    let value: String = op.parameter_into(0)?;
    assert_eq!(value, "some output parameter");

    let mut rs = response.get_resultset()?;
    let value: i32 = rs.pop_row().unwrap().field_into(0)?;
    assert_eq!(value, 42);

    Ok(())
}

fn procedure_with_in_nclob_non_consuming(
    _log_handle: &mut ReconfigurationHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    info!("procedure_with_in_nclob_non_consuming(): convert input parameter to nclob");

    connection.multiple_statements_ignore_err(vec!["drop procedure TEST_CLOB_INPUT_PARS"]);
    connection.multiple_statements(vec![
        "\
         CREATE PROCEDURE \
         TEST_CLOB_INPUT_PARS(IN some_string NCLOB) \
         AS BEGIN \
         SELECT some_string AS \"A\" FROM DUMMY; \
         END;",
    ])?;

    let mut prepared_stmt = connection.prepare("call TEST_CLOB_INPUT_PARS(?)")?;
    let my_parameter = "nclob string".to_string();
    prepared_stmt.add_batch(&my_parameter)?;
    debug!("Still owned {:?}", &my_parameter);
    let mut response = prepared_stmt.execute_batch()?;
    response.get_success()?;
    let mut rs = response.get_resultset()?;
    let mut row = rs.pop_row().unwrap();
    let value: String = row.field_into(0)?;
    assert_eq!(value, "nclob string");

    Ok(())
}
