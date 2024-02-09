use hdbconnect::{Connection, HdbResult, IntoConnectParamsBuilder};

pub fn main() -> HdbResult<()> {
    // Get a connection with default configuration
    let connection = Connection::new(
        "hdbsql://hanahost:39013"
            .into_connect_params_builder()?
            .with_dbuser("HORST")
            .with_password("SECRET"),
    )?;

    // Cleanup if necessary, and set up a test table
    connection.multiple_statements_ignore_err(vec!["drop table FOO_SQUARE"]);
    connection.multiple_statements(vec![
        "create table FOO_SQUARE ( f1 INT primary key, f2 INT)",
    ])?;

    // Insert some test data
    let mut insert_stmt = connection.prepare("insert into FOO_SQUARE (f1, f2) values(?,?)")?;
    for i in 0..100 {
        insert_stmt.add_batch(&(i, i * i))?;
    }
    insert_stmt.execute_batch()?;

    // Read the table data directly into a rust data structure
    let n_square: Vec<(i32, u64)> = connection
        .query("select f1, f2 from FOO_SQUARE order by f1 asc")?
        .try_into()?;

    // Verify ...
    for (idx, (n, square)) in n_square.into_iter().enumerate() {
        println!("{n} * {n} = {square}");
        assert_eq!(idx as i32, n);
        assert_eq!((idx * idx) as u64, square);
    }
    Ok(())
}
