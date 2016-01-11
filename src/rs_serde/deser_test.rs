#![cfg(test)]
use protocol::lowlevel::parts::resultset::{ResultSet,ResultSetCore,Row};
use protocol::lowlevel::part_attributes::PartAttributes;
use protocol::lowlevel::parts::resultset_metadata::{FieldMetadata,ResultSetMetadata};
use protocol::lowlevel::parts::typed_value::TypedValue;
use DbcResult;

use vec_map::VecMap;

#[allow(non_snake_case)]
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct VersionAndUser {
    VERSION: Option<String>,
    CURRENT_USER: String,
}


// cargo test rs_serde::deser_test::test_from_resultset -- --nocapture
#[test]
fn test_from_resultset() {
    // use flexi_logger;
    // flexi_logger::init(flexi_logger::LogConfig::new(),
    //         Some("error,\
    //               hdbconnect::rs_serde=trace,\
    //               ".to_string())).unwrap();

    let resultset = some_resultset();
    let result: DbcResult<Vec<VersionAndUser>> = resultset.into_typed();

    match result {
        Ok(table_content) => info!("ResultSet successfully evaluated: {:?}", table_content),
        Err(e) => {info!("Got an error: {:?}", e); assert!(false)}
    }
}


fn some_resultset() -> ResultSet {
    const NIL: u32 = 4294967295_u32;
    let mut rsm = ResultSetMetadata {
        fields: Vec::<FieldMetadata>::new(),
        names: VecMap::<String>::new(),
    };
    rsm.fields.push( FieldMetadata::new( 2,  9_u8, 0_i16,  32_i16, 0_u32, NIL, 12_u32, 12_u32 ).unwrap() );
    rsm.fields.push( FieldMetadata::new( 1, 11_u8, 0_i16, 256_i16,   NIL, NIL,    NIL, 20_u32 ).unwrap() );

    rsm.names.insert( 0_usize,"M_DATABASE_".to_string());
    rsm.names.insert(12_usize,"VERSION".to_string());
    rsm.names.insert(20_usize,"CURRENT_USER".to_string());

    let mut resultset = ResultSet {
        core_ref: ResultSetCore::new_rs_ref(None, PartAttributes::new(0b_0000_0001), 0_u64),
        metadata: rsm,
        rows: Vec::<Row>::new(),
    };

    resultset.rows.push(Row{values: vec!(
        TypedValue::N_VARCHAR(Some("1.50.000.01.1437580131".to_string())),
        TypedValue::NVARCHAR("SYSTEM".to_string())
    )});
    resultset
}
