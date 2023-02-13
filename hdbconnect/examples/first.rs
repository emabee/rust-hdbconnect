use hdbconnect::{Connection, HdbResult};

#[rustfmt::skip]
pub fn main() -> HdbResult<()> {
    // Get a connection
    let url = "hdbsql://HORST:SECRET@hxehost:39013";
    let connection = Connection::new(url)?;

    // Cleanup if necessary, and set up a test table
    connection.multiple_statements_ignore_err(vec![
        "drop table FOO_SQUARE"
    ]);
    connection.multiple_statements(vec![
        "create table FOO_SQUARE ( f1 INT primary key, f2 INT)",
    ])?;

    // Insert some test data
    let mut insert_stmt = connection.prepare(
        "insert into FOO_SQUARE (f1, f2) values(?,?)"
    )?;

    for i in 0..100 {
        insert_stmt.add_batch(&(i, i * i))?;
    }
    insert_stmt.execute_batch()?;

    // Read the table data directly into a rust data structure
    let stmt = "select * from FOO_SQUARE order by f1 asc";
    let n_square: Vec<(i32, u64)> =
        connection.query(stmt)?.try_into()?;

    // Verify ...
    for (idx, (n, square)) in n_square.into_iter().enumerate() {
        assert_eq!(idx as i32, n);
        assert_eq!((idx * idx) as u64, square);
    }
    Ok(())
}
