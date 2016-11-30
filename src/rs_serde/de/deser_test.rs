#![cfg(test)]
use protocol::lowlevel::parts::resultset::{ResultSet, Row};
use protocol::lowlevel::parts::resultset::factory as ResultSetFactory;
use protocol::lowlevel::parts::resultset_metadata::{FieldMetadata, ResultSetMetadata};
use protocol::lowlevel::parts::typed_value::TypedValue;
use HdbResult;


// cargo test rs_serde::de::deser_test::test_from_resultset -- --nocapture
#[test]
fn test_from_resultset() {
    // use flexi_logger;
    // flexi_logger::LogOptions::new()
    //     .init(Some("trace".to_string()))
    //     .unwrap_or_else(|e| panic!("Logger initialization failed with {}", e));

    info!("minimalistic test of resultset deserialization");

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    pub struct VersionAndUser {
        version: Option<String>,
        current_user: String,
        age: u16,
    }

    let result: HdbResult<Vec<VersionAndUser>> = some_resultset().into_typed();
    match result {
        Err(e) => {
            error!("Got an error: {:?}", e);
            assert!(false)
        }
        Ok(typed_result) => debug!("ResultSet successfully evaluated: {:?}", typed_result),
    }
}

fn some_resultset() -> ResultSet {
    const NIL: u32 = 4294967295_u32;
    let mut rsm = ResultSetMetadata::new_for_tests();
    rsm.fields.push(FieldMetadata::new(2, 9, 0, 32, 0, NIL, 12, 12).unwrap());
    rsm.fields.push(FieldMetadata::new(2, 9, 0, 32, 0, NIL, 20, 20).unwrap());
    rsm.fields.push(FieldMetadata::new(2, 3, 0, 4, 0, NIL, 33, 33).unwrap());

    rsm.names.insert(12, "version".to_string());
    rsm.names.insert(20, "current_user".to_string());
    rsm.names.insert(33, "age".to_string());

    let row = Row {
        values: vec![TypedValue::N_VARCHAR(Some("1.50.000".to_string())),
                     TypedValue::NVARCHAR("HalloDri".to_string()),
                     TypedValue::INT(42)],
    };
    let rows = vec![row.clone(), row.clone(), row.clone()];

    ResultSetFactory::new_for_tests(rsm, rows)
}
