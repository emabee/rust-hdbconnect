extern crate chrono;
extern crate flexi_logger;
extern crate hdbconnect;
#[macro_use]
extern crate log;


extern crate serde_json;

mod test_utils;

use hdbconnect::{Connection, HdbResult, ParameterBinding, ParameterDirection, ResultSet, Row};

#[test] // cargo test test_050_metadata -- --nocapture
pub fn test_050_metadata() {
    test_utils::init_logger("test_050_metadata=info");

    match impl_test_050_metadata() {
        Err(e) => {
            error!("test_050_metadata() failed with {:?}", e);
            assert!(false)
        }
        Ok(n) => info!("{} calls to DB were executed", n),
    }
}


// Test procedures.
// Various procedures from very simple to pretty complex are tested.
fn impl_test_050_metadata() -> HdbResult<i32> {
    let mut connection = test_utils::get_authenticated_connection()?;

    // test procedure with IN, OUT, INOUT parameters
    procedure(&mut connection)?;

    Ok(connection.get_call_count()?)
}

fn procedure(connection: &mut Connection) -> HdbResult<()> {
    info!("procedure(): run a sqlscript procedure with input parameters");

    test_utils::statement_ignore_err(connection, vec!["drop procedure TEST_MD_PARS"]);
    connection.multiple_statements(vec![
        "\
        CREATE  PROCEDURE \
        TEST_MD_PARS( \
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
    assert_eq!(pd0.type_id(), 5);
    assert_eq!(pd0.scale(), 5);
    assert_eq!(pd0.precision(), 10);
    assert_eq!(pd0.direction(), ParameterDirection::INOUT);

    debug!("op-md: {:?}", pd1);
    assert_eq!(pd1.binding(), ParameterBinding::Optional);
    assert_eq!(pd1.name().unwrap(), "OUT_STRING");
    assert_eq!(pd1.type_id(), 11);
    assert_eq!(pd1.scale(), 0);
    assert_eq!(pd1.precision(), 40);
    assert_eq!(pd1.direction(), ParameterDirection::OUT);


    let mut rs: ResultSet = response.get_resultset()?;
    {
        let rs_md = rs.metadata();
        assert_eq!(rs_md.columnname(0)?, "I");
        assert_eq!(rs_md.displayname(0)?, "I");
        assert_eq!(rs_md.has_default(0)?, false);
        assert_eq!(rs_md.is_array_type(0)?, false);
        assert_eq!(rs_md.is_nullable(0)?, true);
        assert_eq!(rs_md.is_readonly(0)?, false);
        assert_eq!(rs_md.precision(0)?, 10);
        assert_eq!(rs_md.scale(0)?, 0);
    }
    let mut row: Row = rs.pop_row().unwrap();
    let value: i32 = row.field_into(0)?;
    assert_eq!(value, 42_i32);
    let value: String = row.field_into(1)?;
    assert_eq!(&value, "is between 41 and 43");

    Ok(())
}
