mod test_utils;

use flexi_logger::ReconfigurationHandle;
use hdbconnect::{
    BaseTypeId, Connection, HdbResult, ParameterBinding, ParameterDirection, ResultSet, Row,
};
use log::{debug, info};

#[test]
pub fn test_050_metadata() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger();
    let mut connection = test_utils::get_authenticated_connection()?;

    test_procedure_metadata(&mut log_handle, &mut connection)?;

    info!("{} calls to DB were executed", connection.get_call_count()?);
    Ok(())
}

fn test_procedure_metadata(
    _log_handle: &mut ReconfigurationHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    info!("procedure(): run a sqlscript procedure with input parameters");

    connection.multiple_statements_ignore_err(vec!["drop procedure TEST_MD_PARS"]);
    connection.multiple_statements(vec![
        "\
         CREATE  PROCEDURE TEST_MD_PARS( \
         IN in_int INT, \
         IN in_string NVARCHAR(20), \
         INOUT inout_decimal DECIMAL(10,5), \
         OUT out_string NVARCHAR(40) \
         ) \
         AS BEGIN \
         SELECT in_int AS \"I\", in_string AS \"A\" FROM DUMMY; \
         inout_decimal = inout_decimal * inout_decimal; \
         out_string = 'some output parameter'; \
         END;",
    ])?;

    let mut prepared_stmt = connection.prepare("call TEST_MD_PARS(?,?,?,?)")?;
    prepared_stmt.add_batch(&(42, "is between 41 and 43", 23.45_f32))?;
    let mut response = prepared_stmt.execute_batch()?;

    response.get_affected_rows()?;

    let op = response.get_output_parameters()?;
    let pd0 = op.parameter_descriptor(0)?;
    let pd1 = op.parameter_descriptor(1)?;
    debug!("op-md: {:?}", pd0);
    assert_eq!(pd0.binding(), ParameterBinding::Optional);
    assert_eq!(pd0.name().unwrap(), "INOUT_DECIMAL");
    assert_eq!(pd0.type_id().base_type_id(), &BaseTypeId::DECIMAL);
    assert_eq!(pd0.scale(), 5);
    assert_eq!(pd0.precision(), 10);
    assert_eq!(pd0.direction(), ParameterDirection::INOUT);

    debug!("op-md: {:?}", pd1);
    assert_eq!(pd1.binding(), ParameterBinding::Optional);
    assert_eq!(pd1.name().unwrap(), "OUT_STRING");
    assert_eq!(pd1.type_id().base_type_id(), &BaseTypeId::NVARCHAR);
    assert_eq!(pd1.scale(), 0);
    assert_eq!(pd1.precision(), 40);
    assert_eq!(pd1.direction(), ParameterDirection::OUT);

    let mut rs: ResultSet = response.get_resultset()?;
    let mut row: Row = rs.pop_row().unwrap();
    let value: i32 = row.field_into(0)?;
    assert_eq!(value, 42_i32);
    let value: String = row.field_into(1)?;
    assert_eq!(&value, "is between 41 and 43");

    let rs_md = rs.metadata();
    assert_eq!(rs_md.columnname(0)?, "I");
    assert_eq!(rs_md.displayname(0)?, "I");
    assert_eq!(rs_md.has_default(0)?, false);
    assert_eq!(rs_md.is_array_type(0)?, false);
    assert_eq!(rs_md.is_nullable(0)?, true);
    assert_eq!(rs_md.is_readonly(0)?, false);
    assert_eq!(rs_md.precision(0)?, 10);
    assert_eq!(rs_md.scale(0)?, 0);
    Ok(())
}
