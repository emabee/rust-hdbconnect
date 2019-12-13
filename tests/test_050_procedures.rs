mod test_utils;

use flexi_logger::ReconfigurationHandle;
use hdbconnect::{
    Connection, HdbResult, HdbReturnValue, ParameterBinding, ParameterDirection, ResultSet, Row,
    TypeId,
};
use log::{debug, info};

// Test various procedures, from very simple to pretty complex
#[test]
pub fn test_050_procedures() -> HdbResult<()> {
    let start = std::time::Instant::now();
    let mut log_handle = test_utils::init_logger();
    let mut connection = test_utils::get_authenticated_connection()?;

    very_simple_procedure(&mut log_handle, &mut connection)?;
    procedure_with_out_resultsets(&mut log_handle, &mut connection)?;
    procedure_with_secret_resultsets(&mut log_handle, &mut connection)?;
    procedure_with_in_parameters(&mut log_handle, &mut connection)?;
    procedure_with_in_and_out_parameters(&mut log_handle, &mut connection)?;
    procedure_with_in_nclob_non_consuming(&mut log_handle, &mut connection)?;
    procedure_with_in_nclob_and_out_nclob(&mut log_handle, &mut connection)?;

    info!("{} calls to DB were executed", connection.get_call_count()?);
    info!(
        "Elapsed time: {:?}, Accumulated server processing time: {:?}",
        std::time::Instant::now().duration_since(start),
        connection.server_usage()?.accum_proc_time
    );
    Ok(())
}

fn very_simple_procedure(
    _log_handle: &mut ReconfigurationHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    info!("very_simple_procedure(): run a simple sqlscript procedure");

    connection.multiple_statements(vec![
        "\
         CREATE OR REPLACE PROCEDURE TEST_PROCEDURE \
         LANGUAGE SQLSCRIPT SQL SECURITY DEFINER \
         AS BEGIN \
         SELECT CURRENT_USER \"current user\" FROM DUMMY; \
         END",
    ])?;

    let mut response = connection.statement("call TEST_PROCEDURE")?;
    response.get_success()?;
    let _resultset = response.get_resultset()?;
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

    connection.multiple_statements(vec![
        "\
         CREATE OR REPLACE PROCEDURE \
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

    connection.multiple_statements(vec![
        "\
         CREATE OR REPLACE PROCEDURE \
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

    let mut response: hdbconnect::HdbResponse =
        connection.statement("call GET_PROCEDURES_SECRETLY()")?;
    response.reverse();
    for ret_val in response {
        match ret_val {
            HdbReturnValue::ResultSet(rs) => debug!("Got a resultset: {:?}", rs),
            HdbReturnValue::AffectedRows(affected_rows) => {
                debug!("Got affected_rows: {:?}", affected_rows)
            }
            HdbReturnValue::Success => debug!("Got success"),
            HdbReturnValue::OutputParameters(output_parameters) => {
                debug!("Got output_parameters: {:?}", output_parameters)
            }
            HdbReturnValue::XaTransactionIds(_) => debug!("cannot happen"),
        }
    }

    Ok(())
}

fn procedure_with_in_parameters(
    _log_handle: &mut ReconfigurationHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    info!("procedure_with_in_parameters(): run a sqlscript procedure with input parameters");

    connection.multiple_statements(vec![
        "\
         CREATE OR REPLACE PROCEDURE \
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
    let mut row: Row = rs.next_row()?.unwrap();
    let value: i32 = row.next_value().unwrap().try_into()?;
    assert_eq!(value, 42_i32);
    let value: String = row.next_value().unwrap().try_into()?;
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

    connection.multiple_statements(vec![
        "CREATE OR REPLACE PROCEDURE \
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
    let output_parameters = response.get_output_parameters()?;
    {
        let par_desc = output_parameters.descriptor(0)?;
        assert_eq!(par_desc.binding(), ParameterBinding::Optional);
        assert_eq!(par_desc.type_id(), TypeId::NVARCHAR);
        assert_eq!(par_desc.direction(), ParameterDirection::OUT);
        assert_eq!(par_desc.name(), Some(&"SOME_STRING".to_string()));
    }
    let value: String = output_parameters.try_into()?;
    assert_eq!(value, "some output parameter");

    let mut rs = response.get_resultset()?;
    let value: i32 = rs.next_row()?.unwrap().next_value().unwrap().try_into()?;
    assert_eq!(value, 42);

    Ok(())
}

fn procedure_with_in_nclob_non_consuming(
    _log_handle: &mut ReconfigurationHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    info!("procedure_with_in_nclob_non_consuming(): convert input parameter to nclob");

    connection.multiple_statements(vec![
        "\
         CREATE OR REPLACE PROCEDURE \
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
    let mut row = rs.next_row()?.unwrap();
    let value: String = row.next_value().unwrap().try_into()?;
    assert_eq!(value, "nclob string");

    Ok(())
}

use hdbconnect::{types::NCLob, HdbValue};

fn procedure_with_in_nclob_and_out_nclob(
    _log_handle: &mut ReconfigurationHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    info!("procedure_with_in_nclob_and_out_nclob");
    // FIXME switch to a self-written procedure, EXECUTE_MDS is moved into an AFL and
    // might not be available
    let mut stmt = connection.prepare("CALL SYS.EXECUTE_MDS(?, '','','','', ?,?)")?;

    let reader = std::sync::Arc::new(std::sync::Mutex::new(std::io::Cursor::new(
        "Hello World!".to_string(),
    )));

    _log_handle.parse_new_spec("info, test=debug");
    let mut result = stmt.execute_row(vec![
        HdbValue::STRING("Analytics".to_string()),
        HdbValue::LOBSTREAM(Some(reader)),
    ])?;

    let response: NCLob = result
        .get_output_parameters()?
        .values_mut()
        .next()
        .unwrap()
        .try_into_nclob()?;

    debug!("{}", response.into_string()?);
    Ok(())
}
