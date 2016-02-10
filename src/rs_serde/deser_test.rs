#![cfg(test)]
use protocol::lowlevel::parts::resultset::{ResultSet,Row};
use protocol::lowlevel::parts::resultset::factory as ResultSetFactory;
use protocol::lowlevel::parts::resultset_metadata::{FieldMetadata,ResultSetMetadata};
use protocol::lowlevel::parts::typed_value::TypedValue;
use DbcResult;
use flexi_logger::{self,LogConfig,opt_format};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct VersionAndUser {
    version: Option<String>,
    current_user: String,
}

// cargo test rs_serde::deser_test::test_from_resultset -- --nocapture
#[test]
fn test_from_resultset() {
    flexi_logger::init(
            LogConfig{
                log_to_file: false,
                format: opt_format,
                .. LogConfig::new()
            },
            Some("info,\
                 ".to_string())
    ).unwrap();

    let result: DbcResult<Vec<VersionAndUser>> = some_resultset().into_typed();
    match result {
        Err(e) => {error!("Got an error: {:?}", e); assert!(false)},
        Ok(typed_result) => info!("ResultSet successfully evaluated: {:?}", typed_result),
    }
}

fn some_resultset() -> ResultSet {
    const NIL: u32 = 4294967295_u32;
    let mut rsm = ResultSetMetadata::new();
    rsm.fields.push( FieldMetadata::new( 2,  9_u8, 0_i16,  32_i16, 0_u32, NIL, 12_u32, 12_u32 ).unwrap() );

    rsm.names.insert( 0_usize,"M_DATABASE_".to_string());
    rsm.names.insert(12_usize,"version".to_string());
    rsm.names.insert(20_usize,"current_user".to_string());

    let mut resultset = ResultSetFactory::new_for_tests(rsm);

    resultset.rows.push(Row{values: vec!(
        TypedValue::N_VARCHAR(Some("1.50.000.01.1437580131".to_string())),
        TypedValue::NVARCHAR("SYSTEM".to_string())
    )});
    resultset
}
