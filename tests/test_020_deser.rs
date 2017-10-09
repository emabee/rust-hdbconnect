extern crate chrono;
extern crate flexi_logger;
extern crate hdbconnect;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

mod test_utils;

use chrono::NaiveDateTime;
use hdbconnect::{Connection, HdbResult};

#[test] // cargo test --test test_020_deser -- --nocapture
pub fn deser() {
    test_utils::init_logger("test_020_deser=info");

    match impl_deser() {
        Err(e) => {
            error!("impl_deser() failed with {:?}", e);
            assert!(false)
        }
        Ok(i) => info!("{} calls to DB were executed", i),
    }
}


// Test the graceful conversion during deserialization,
// in regards to nullable fields, and to simplified result structures
fn impl_deser() -> HdbResult<i32> {
    let mut connection = test_utils::get_authenticated_connection()?;

    deser_option_into_option(&mut connection)?;
    deser_plain_into_plain(&mut connection)?;
    deser_plain_into_option(&mut connection)?;
    deser_option_into_plain(&mut connection)?;

    deser_singleline_into_struct(&mut connection)?;
    deser_singlecolumn_into_vec(&mut connection)?;
    deser_singlevalue_into_plain(&mut connection)?;

    Ok(connection.get_call_count()?)
}


#[derive(Deserialize, Debug)]
struct TS<S, I, D> {
    #[serde(rename = "F1_S")]
    f1_s: S,
    #[serde(rename = "F2_I")]
    f2_i: I,
    #[serde(rename = "F3_D")]
    f3_d: D,
}



fn deser_option_into_option(connection: &mut Connection) -> HdbResult<()> {
    info!("deserialize Option values into Option values, test null and not-null values");
    test_utils::statement_ignore_err(connection, vec!["drop table TEST_DESER_OPT_OPT"]);
    let stmts = vec!["create table TEST_DESER_OPT_OPT \
                        (f1_s NVARCHAR(10), f2_i INT, f3_d LONGDATE)",
                     "insert into TEST_DESER_OPT_OPT (f1_s) values('hello')",
                     "insert into TEST_DESER_OPT_OPT (f2_i) values(17)",
                     "insert into TEST_DESER_OPT_OPT (f3_d) values('01.01.1900')"];
    connection.multiple_statements(stmts)?;

    type TestStruct = TS<Option<String>, Option<i32>, Option<NaiveDateTime>>;

    let resultset = connection.query("select * from TEST_DESER_OPT_OPT")?;
    let typed_result: Vec<TestStruct> = resultset.into_typed()?;

    assert_eq!(typed_result.len(), 3);
    Ok(())
}

fn deser_plain_into_plain(connection: &mut Connection) -> HdbResult<()> {
    info!("deserialize plain values into plain values");
    test_utils::statement_ignore_err(connection, vec!["drop table TEST_DESER_PLAIN_PLAIN"]);
    let stmts = vec!["create table TEST_DESER_PLAIN_PLAIN \
                        (F1_S NVARCHAR(10) not null, F2_I INT not null, F3_D LONGDATE not null)",
                     "insert into TEST_DESER_PLAIN_PLAIN values('hello', 17, '01.01.1900')",
                     "insert into TEST_DESER_PLAIN_PLAIN values('little', 18, '01.01.2000')",
                     "insert into TEST_DESER_PLAIN_PLAIN values('world', 19, '01.01.2100')"];
    connection.multiple_statements(stmts)?;

    type TestStruct = TS<String, i32, NaiveDateTime>;

    let resultset = connection.query("select * from TEST_DESER_PLAIN_PLAIN")?;
    let typed_result: Vec<TestStruct> = resultset.into_typed()?;

    assert_eq!(typed_result.len(), 3);
    Ok(())
}

fn deser_plain_into_option(connection: &mut Connection) -> HdbResult<()> {
    info!("deserialize plain values into Option values");
    test_utils::statement_ignore_err(connection, vec!["drop table TEST_DESER_PLAIN_OPT"]);
    let stmts = vec!["create table TEST_DESER_PLAIN_OPT \
                        (F1_S NVARCHAR(10) not null, F2_I INT not null, F3_D LONGDATE not null)",
                     "insert into TEST_DESER_PLAIN_OPT values('hello', 17, '01.01.1900')",
                     "insert into TEST_DESER_PLAIN_OPT values('little', 18, '01.01.2000')",
                     "insert into TEST_DESER_PLAIN_OPT values('world', 19, '01.01.2100')"];
    connection.multiple_statements(stmts)?;

    type TestStruct = TS<Option<String>, Option<i32>, Option<NaiveDateTime>>;

    let resultset = connection.query("select * from TEST_DESER_PLAIN_OPT")?;
    let typed_result: Vec<TestStruct> = resultset.into_typed()?;

    assert_eq!(typed_result.len(), 3);
    Ok(())
}

fn deser_option_into_plain(connection: &mut Connection) -> HdbResult<()> {
    info!("deserialize Option values into plain values, test not-null values; test that null \
           values fail");
    test_utils::statement_ignore_err(connection, vec!["drop table TEST_DESER_OPT_PLAIN"]);
    let stmts = vec!["create table TEST_DESER_OPT_PLAIN \
                        (F1_S NVARCHAR(10), F2_I INT, F3_D LONGDATE)"];
    connection.multiple_statements(stmts)?;

    type TestStruct = TS<String, i32, NaiveDateTime>;

    // first part: no null values, this must work
    let stmts = vec!["insert into TEST_DESER_OPT_PLAIN values('hello', 17, '01.01.1900')",
                     "insert into TEST_DESER_OPT_PLAIN values('little', 18, '01.01.2000')",
                     "insert into TEST_DESER_OPT_PLAIN values('world', 19, '01.01.2100')"];
    connection.multiple_statements(stmts)?;

    let resultset = connection.query("select * from TEST_DESER_OPT_PLAIN")?;
    let typed_result: Vec<TestStruct> = resultset.into_typed()?;
    assert_eq!(typed_result.len(), 3);

    // second part: with null values, deserialization must fail
    let stmts = vec!["insert into TEST_DESER_OPT_PLAIN (F2_I) values(17)"];
    connection.multiple_statements(stmts)?;

    let resultset = connection.query("select * from TEST_DESER_OPT_PLAIN")?;
    let _typed_result: HdbResult<Vec<TestStruct>> = resultset.into_typed();
    if let Ok(_) = _typed_result {
        panic!("deserialization of null values to plain data fields did not fail")
    }

    Ok(())
}

fn deser_singleline_into_struct(connection: &mut Connection) -> HdbResult<()> {
    info!("deserialize a single-line resultset into a struct; test that this is not possible \
           with multi-line resultsets");
    test_utils::statement_ignore_err(connection, vec!["drop table TEST_DESER_SINGLE_LINE"]);
    let stmts = vec!["create table TEST_DESER_SINGLE_LINE \
                        (F1_S NVARCHAR(10), F2_I INT, F3_D LONGDATE)",
                     "insert into TEST_DESER_SINGLE_LINE (F1_S) values('hello')",
                     "insert into TEST_DESER_SINGLE_LINE (F2_I) values(17)",
                     "insert into TEST_DESER_SINGLE_LINE (F3_D) values('01.01.1900')"];
    connection.multiple_statements(stmts)?;

    type TestStruct = TS<Option<String>, Option<i32>, Option<NaiveDateTime>>;

    // single line works
    let resultset = connection.query("select * from TEST_DESER_SINGLE_LINE where F2_I = 17")?;
    let typed_result: TestStruct = resultset.into_typed()?;
    assert_eq!(typed_result.f2_i, Some(17));

    // multi-line fails
    let resultset = connection.query("select * from TEST_DESER_SINGLE_LINE")?;
    let _typed_result: HdbResult<TestStruct> = resultset.into_typed();
    if let Ok(_) = _typed_result {
        panic!("deserialization of a multiline resultset to a plain struct did not fail")
    }

    Ok(())
}

fn deser_singlevalue_into_plain(connection: &mut Connection) -> HdbResult<()> {
    info!("deserialize a single-value resultset into a plain field; \
           test that this is not possible with multi-line or multi-column resultsets");
    test_utils::statement_ignore_err(connection, vec!["drop table TEST_DESER_SINGLE_VALUE"]);
    let stmts = vec!["create table TEST_DESER_SINGLE_VALUE \
                        (F1_S NVARCHAR(10), F2_I INT, F3_D LONGDATE)",
                     "insert into TEST_DESER_SINGLE_VALUE (F1_S) values('hello')",
                     "insert into TEST_DESER_SINGLE_VALUE (F2_I) values(17)",
                     "insert into TEST_DESER_SINGLE_VALUE (F3_D) values('01.01.1900')"];
    connection.multiple_statements(stmts)?;

    // single value works
    let resultset = connection.query("select F2_I from TEST_DESER_SINGLE_VALUE \
                                                            where F2_I = 17")?;
    let _typed_result: i64 = resultset.into_typed()?;

    // multi-col fails
    let resultset = connection.query("select F2_I, F2_I from TEST_DESER_SINGLE_VALUE \
                                                                  where F2_I = 17")?;
    let _typed_result: HdbResult<i64> = resultset.into_typed();
    if let Ok(_) = _typed_result {
        panic!("deserialization of a multi-column resultset into a plain field did not fail")
    }

    // multi-row fails
    let resultset = connection.query("select F2_I from TEST_DESER_SINGLE_VALUE")?;
    let typed_result: HdbResult<i64> = resultset.into_typed();
    if let Ok(_) = typed_result {
        panic!("deserialization of a multi-row resultset into a plain field did not fail")
    }

    Ok(())
}

fn deser_singlecolumn_into_vec(connection: &mut Connection) -> HdbResult<()> {
    info!("deserialize a single-column resultset into a Vec of plain fields; \
           test that multi-column resultsets fail");

    test_utils::statement_ignore_err(connection, vec!["drop table TEST_DESER_SINGLE_COL"]);
    let stmts = vec!["create table TEST_DESER_SINGLE_COL (F1_S NVARCHAR(10), F2_I int)",
                     "insert into TEST_DESER_SINGLE_COL (F1_S, F2_I) values('hello', 0)",
                     "insert into TEST_DESER_SINGLE_COL (F1_S, F2_I) values('world', 1)",
                     "insert into TEST_DESER_SINGLE_COL (F1_S, F2_I) values('here', 2)",
                     "insert into TEST_DESER_SINGLE_COL (F1_S, F2_I) values('I', 3)",
                     "insert into TEST_DESER_SINGLE_COL (F1_S, F2_I) values('am', 4)"];
    connection.multiple_statements(stmts)?;

    // single-column works
    let resultset = connection.query("select F1_S \
                                                from TEST_DESER_SINGLE_COL order by F2_I asc")?;
    let typed_result: Vec<String> = resultset.into_typed()?;
    assert_eq!(typed_result.len(), 5);

    // multi-column fails
    let resultset = connection.query("select F1_S, F1_S \
                                                from TEST_DESER_SINGLE_COL order by F2_I asc")?;
    let typed_result: HdbResult<Vec<String>> = resultset.into_typed();
    if let Ok(_) = typed_result {
        panic!("deserialization of a multi-column resultset into a Vec<plain field> did not fail");
    }

    Ok(())
}
