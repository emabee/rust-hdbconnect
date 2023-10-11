extern crate serde;

mod test_utils;

use flexi_logger::LoggerHandle;
use hdbconnect_async::{
    Connection, HdbResult, ParameterBinding, ParameterDirection, ResultSet, Row, TypeId,
};
use log::{debug, info};

#[tokio::test]
async fn test_060_metadata() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger();
    let start = std::time::Instant::now();
    let connection = test_utils::get_authenticated_connection().await?;

    test_procedure_metadata(&mut log_handle, &connection).await?;

    test_utils::closing_info(connection, start).await
}

#[allow(clippy::cognitive_complexity)]
async fn test_procedure_metadata(
    _log_handle: &mut LoggerHandle,
    connection: &Connection,
) -> HdbResult<()> {
    info!("procedure(): run a sqlscript procedure with input parameters");

    connection
        .multiple_statements_ignore_err(vec!["drop procedure TEST_MD_PARS"])
        .await;
    connection
        .multiple_statements(vec![
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
        ])
        .await?;

    let mut prepared_stmt = connection.prepare("call TEST_MD_PARS(?,?,?,?)").await?;
    prepared_stmt.add_batch(&(42, "is between 41 and 43", 23.45_f32))?;
    let mut response = prepared_stmt.execute_batch().await?;

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
    let row: Row = rs.next_row().await?.unwrap();
    assert_eq!(row[0], 42_i32);
    assert_eq!(row[1], "is between 41 and 43");

    let rs_md = rs.metadata();
    assert_eq!(rs_md[0].columnname(), "I");
    assert_eq!(rs_md[0].displayname(), "I");
    assert!(!rs_md[0].has_default());
    assert!(!rs_md[0].is_array_type());
    assert!(rs_md[0].is_nullable());
    assert!(!rs_md[0].is_read_only());
    assert_eq!(rs_md[0].precision(), 10);
    assert_eq!(rs_md[0].scale(), 0);
    Ok(())
}
