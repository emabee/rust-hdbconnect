#[macro_use]
extern crate serde;

mod test_utils;

use flexi_logger::LoggerHandle;
use hdbconnect::{Connection, HdbResult, HdbValue};

#[test] // cargo test --test test_029_arrays -- --nocapture
pub fn test_029_arrays() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger();
    let start = std::time::Instant::now();
    let mut connection = test_utils::get_authenticated_connection()?;

    test_arrays(&mut log_handle, &mut connection)?;

    test_utils::closing_info(connection, start)
}

fn test_arrays(log_handle: &mut LoggerHandle, connection: &mut Connection) -> HdbResult<()> {
    log::debug!("prepare the db tables");
    connection.multiple_statements_ignore_err(vec![
        "drop table TEST_INTEGER_ARRAYS",
        "drop table TEST_STRING_ARRAYS",
    ]);
    let stmts = vec![
        "create table TEST_INTEGER_ARRAYS (ID INT, VAL INT ARRAY)",
        "create table TEST_STRING_ARRAYS (ID INT, VAL NVARCHAR(20) ARRAY)",
        "INSERT INTO TEST_INTEGER_ARRAYS VALUES (1, ARRAY(1, 2, 3))",
        "INSERT INTO TEST_INTEGER_ARRAYS VALUES (2, ARRAY(26214, 10, 0, 2147483647))",
        "INSERT INTO TEST_STRING_ARRAYS VALUES \
         (2, ARRAY('Hello ', 'world, ', 'this ', 'is ', 'not ', 'the ', 'end!'))",
    ];
    connection.multiple_statements(stmts)?;

    // This seems to be not doable:
    //     connection.prepare("insert into TEST_INTEGER_ARRAYS values(?,?)")
    // The preparation fails with
    //      error [code: 266, sql state: 07006] at position: 0:
    //      "inconsistent datatype: ARRAY type is incompatible with PARAMETER type:
    //       line 1 col 42 (at pos 41)"):

    // Only this is doable, but it's boring ...
    let stmt = connection
        .prepare("insert into TEST_INTEGER_ARRAYS values(?,ARRAY(?,?))")
        .unwrap();

    log::info!("Metadata: {:?}", *stmt.parameter_descriptors());

    let mut s = String::with_capacity(31_000);
    s.push_str("INSERT INTO TEST_INTEGER_ARRAYS VALUES (3, ARRAY(26214, 10, 0, 2147483647");
    for _ in 0..10_000 {
        s.push_str(", 7");
    }
    s.push_str("))");
    assert_eq!(connection.dml(s)?, 1);

    let cards: Vec<u32> = connection
        .query(r#"SELECT CARDINALITY(VAL) "cardinality" FROM TEST_INTEGER_ARRAYS"#)?
        .try_into()?;
    assert_eq!(cards, vec![3, 4, 10_004]);

    let value = connection
        .query("select val from TEST_INTEGER_ARRAYS where id = 2")?
        .into_single_row()?
        .into_single_value()?;

    if let HdbValue::ARRAY(vec) = value {
        assert_eq!(vec, vec![26214, 10, 0, 2_147_483_647]);
    } else {
        panic!("not an array");
    }

    log_handle
        .parse_new_spec("info, hdbconnect::protocol::parts::hdb_value = debug")
        .unwrap();

    let value = connection
        .query("select val from TEST_INTEGER_ARRAYS where id = 3")?
        .into_single_row()?
        .into_single_value()?;

    if let HdbValue::ARRAY(vec) = value {
        assert_eq!(vec.len(), 10_004);
    } else {
        panic!("not an array");
    }

    let value = connection
        .query("select val from TEST_STRING_ARRAYS where id = 2")?
        .into_single_row()?
        .into_single_value()?;

    if let HdbValue::ARRAY(vec) = value {
        assert_eq!(vec.len(), 7);
        let vec: Vec<String> = vec
            .into_iter()
            .map(|hdbval| hdbval.try_into().unwrap())
            .collect();
        assert_eq!(vec.as_slice().concat(), "Hello world, this is not the end!");
    } else {
        panic!("not an array");
    }

    Ok(())
}
