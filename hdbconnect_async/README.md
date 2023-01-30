# hdbconnect

[![crates.io](https://meritbadge.herokuapp.com/hdbconnect)](https://crates.io/crates/hdbconnect)
[![docs](https://docs.rs/hdbconnect/badge.svg)](https://docs.rs/hdbconnect)
![License](https://img.shields.io/crates/l/hdbconnect.svg)

A synchronous pure rust SQL driver for SAP HANA(TM).

## Usage

Add hdbconnect to the dependencies section in your project's `Cargo.toml`:

```toml
[dependencies]
hdbconnect = "0.25"
```

Assume you have a HANA accessible at port `39013` on host `hxehost`,
and you can log on to it as user `HORST` with password `SeCrEt`.

Then a first simple test might look like this:

//FIXME test this

```rust
use hdbconnect_async::{Connection, HdbResult};

#[tokio::main]
pub async fn main() -> HdbResult<()> {
    let mut connection = Connection::new("hdbsql://HORST:SeCrEt@hxehost:39013").await?;

    // Cleanup if necessary, and set up a test table
    connection.multiple_statements_ignore_err(vec![
        "drop table FOO_SQUARE"
    ]).await;
    connection.multiple_statements(vec![
        "create table FOO_SQUARE ( f1 INT primary key, f2 INT)",
    ]).await?;

    // Insert some test data
    let mut insert_stmt = connection.prepare(
        "insert into FOO_SQUARE (f1, f2) values(?,?)"
    ).await?;

    for i in 0..100 {
        insert_stmt.add_batch(&(i, i * i))?;
    }
    insert_stmt.execute_batch().await?;

    // Read the table data directly into a rust data structure
    let stmt = "select * from FOO_SQUARE order by f1 asc";
    let n_square: Vec<(i32, u64)> =
        connection.query(stmt).await?.try_into().await?;

    // Verify ...
    for (idx, (n, square)) in n_square.into_iter().enumerate() {
        assert_eq!(idx as i32, n);
        assert_eq!((idx * idx) as u64, square);
    }
    Ok(())
}
```

## Documentation

FIXME
See <https://docs.rs/hdbconnect/> for the full functionality of hdbconnect.

There you also find more code examples, e.g. in the description of module `code_examples`.

## TLS

See [HANA in SCP](HANA_in_SCP.md) for instructions how to obtain the necessary server
certificate from a HANA in SAP Cloud Platform.
