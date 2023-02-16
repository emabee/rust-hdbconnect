# hdbconnect

[![Latest version](https://img.shields.io/crates/v/hdbconnect.svg)](https://crates.io/crates/hdbconnect)
[![Documentation](https://docs.rs/hdbconnect/badge.svg)](https://docs.rs/hdbconnect)
[![License](https://img.shields.io/crates/l/hdbconnect.svg)](https://github.com/emabee/hdbconnect)

A synchronous pure rust SQL driver for SAP HANA(TM).

Check out [`hdbconnect_async`](https://crates.io/crates/hdbconnect_async)
if you need an asynchronous driver for SAP HANA.

## Usage

Add `hdbconnect` to the dependencies section in your project's `Cargo.toml`:

```toml
[dependencies]
hdbconnect = "0.26"
```

Assuming you have

- a HANA accessible at port `39013` on host `hxehost`,
- and you can log on to it as user `HORST` with password `SeCrEt`,

then a first simple test which sets up some table, inserts data and reads them back
might look like this:

```rust
use hdbconnect::{Connection, HdbResult};

pub fn main() -> HdbResult<()> {
    let mut connection = Connection::new("hdbsql://HORST:SeCrEt@hxehost:39013")?;

    // Cleanup if necessary, and set up a test table
    connection.multiple_statements_ignore_err(vec![
        "drop table FOO_SQUARE"
    ]);
    connection.multiple_statements(vec![
        "create table FOO_SQUARE ( f1 INT primary key, f2 BIGINT)",
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
```

## Documentation

The [docs](https://docs.rs/hdbconnect/) contain more code examples,
especially in the description of module `code_examples`.

## TLS

See [HANA in SCP](https://github.com/emabee/rust-hdbconnect/blob/master/HANA_in_SCP.md)
for instructions how to obtain the necessary server certificate from a HANA in SAP Cloud Platform.

## Features

### `r2d2_pool`

Adds an implementation of a [`r2d2`](https://crates.io/crates/r2d2) database pool.

### `dist_tx`

Adds support for distributed transactions, based on [`dist_tx`](https://crates.io/crates/dist_tx).

## Versions

See the [change log](https://github.com/emabee/rust-hdbconnect/blob/master/CHANGELOG.md).
