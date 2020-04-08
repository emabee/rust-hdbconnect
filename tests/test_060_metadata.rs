#[macro_use]
extern crate serde;

mod test_utils;

use flexi_logger::ReconfigurationHandle;
use hdbconnect::{Connection, HdbResult, ParameterBinding, ParameterDirection, ResultSet, TypeId};
use log::{debug, info};

#[test]
pub fn test_060_metadata() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger();
    let start = std::time::Instant::now();
    let mut connection = test_utils::get_authenticated_connection()?;

    test_procedure_metadata(&mut log_handle, &mut connection)?;

    test_utils::closing_info(connection, start)
}

#[allow(clippy::cognitive_complexity)]
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

    let output_parameters = response.get_output_parameters()?;
    let pd0 = &output_parameters.descriptors()[0];
    let pd1 = &output_parameters.descriptors()[1];
    debug!("op-md: {:?}", pd0);
    assert_eq!(pd0.binding(), ParameterBinding::Optional);
    assert_eq!(pd0.name().unwrap(), "INOUT_DECIMAL");
    // behavior depends on DB version:
    assert!((pd0.type_id() == TypeId::FIXED8) | (pd0.type_id() == TypeId::DECIMAL));
    assert_eq!(pd0.scale(), 5);
    assert_eq!(pd0.precision(), 10);
    assert_eq!(pd0.direction(), ParameterDirection::INOUT);

    debug!("op-md: {:?}", pd1);
    assert_eq!(pd1.binding(), ParameterBinding::Optional);
    assert_eq!(pd1.name().unwrap(), "OUT_STRING");
    assert_eq!(pd1.type_id(), TypeId::NVARCHAR);
    assert_eq!(pd1.scale(), 0);
    assert_eq!(pd1.precision(), 40);
    assert_eq!(pd1.direction(), ParameterDirection::OUT);

    let mut rs: ResultSet = response.get_resultset()?;
    let row: hdbconnect::Row = rs.next_row()?.unwrap();
    assert_eq!(row[0], 42_i32);
    assert_eq!(row[1], "is between 41 and 43");

    let rs_md = rs.metadata();
    assert_eq!(rs_md.columnname(0)?, "I");
    assert_eq!(rs_md.displayname(0)?, "I");
    assert_eq!(rs_md.has_default(0)?, false);
    assert_eq!(rs_md.is_array_type(0)?, false);
    assert_eq!(rs_md.nullable(0)?, true);
    assert_eq!(rs_md.read_only(0)?, false);
    assert_eq!(rs_md.precision(0)?, 10);
    assert_eq!(rs_md.scale(0)?, 0);
    Ok(())
}
