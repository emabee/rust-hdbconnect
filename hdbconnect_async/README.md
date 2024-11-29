# hdbconnect_async

[![Latest version](https://img.shields.io/crates/v/hdbconnect_async.svg)](https://crates.io/crates/hdbconnect_async)
[![Documentation](https://docs.rs/hdbconnect_async/badge.svg)](https://docs.rs/hdbconnect_async)
[![License](https://img.shields.io/crates/l/hdbconnect_async.svg)](https://github.com/emabee/hdbconnect_async)

An asynchronous pure rust SQL driver for SAP HANA(TM).

Use [`hdbconnect`](https://crates.io/crates/hdbconnect)
if you want a synchronous driver for SAP HANA.

## Usage

Add `hdbconnect_async` to the dependencies section in your project's `Cargo.toml`:

```toml
[dependencies]
hdbconnect_async = "0.30"
```

Assuming you have

- a HANA accessible at port `39013` on host `hxehost`,
- and you can log on to it as user `HORST` with password `SeCrEt`,

then a first simple test which sets up some table, inserts data and reads them back
might look like this:

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
        "create table FOO_SQUARE ( f1 INT primary key, f2 BIGINT)",
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

See <https://docs.rs/hdbconnect_async/> for the full functionality of hdbconnect_async.

There you find also more code examples, especially in the description of module `code_examples`.

## TLS

The TLS implementation is based on `rustls`.

`rustls`'s flexibility to use non-default crypto providers is currently not (yet) exposed by `hdbconnect`.
Nevertheless, the need of `rustls` to initialize crypto shines through.
We thus recommend calling `hdbconnect::initialize_crypto` early in your main.

## Features

### `rocket_pool`

Adds an implementation of a [`rocket_db_pools`](https://crates.io/crates/rocket_db_pools) database pool.

### `dist_tx`

Adds support for distributed transactions, based on [`dist_tx`](https://crates.io/crates/dist_tx).

## Versions

See the [change log](https://github.com/emabee/rust-hdbconnect/blob/master/CHANGELOG.md).
