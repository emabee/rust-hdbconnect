## Contents

- [Database connections](#database-connections)
- [Queries and other database calls](#queries-and-other-database-calls)
  - [Generic method: `Connection::statement()` and `HdbResponse`](#generic-method-connectionstatement-and-hdbresponse)
  - [More specific methods with more convenient return values](#more-specific-methods-with-more-convenient-return-values)
  - [Prepared statements](#prepared-statements)
- [Iterating over rows](#iterating-over-rows)
- [Result set evaluation with `try_into()`](#result-set-evaluation-with-try_into)
  - [Explicitly evaluating rows](#explicitly-evaluating-rows)
  - [Direct conversion of entire rows](#direct-conversion-of-entire-rows)
  - [Direct conversion of entire result sets](#direct-conversion-of-entire-result-sets)
    - [Matrix-structured result sets](#matrix-structured-result-sets)
    - [Single-line result sets](#single-line-result-sets)
    - [Single-column result sets](#single-column-result-sets)
    - [Single-value result sets](#single-value-result-sets)
- [Deserialization of field values](#deserialization-of-field-values)
- [Binary Values](#binary-values)
- [LOBs](#lobs)
  - [Streaming LOBs to the database](#streaming-lobs-to-the-database)
  - [Streaming LOBs from the database](#streaming-lobs-from-the-database)

## Database connections

To establish an authenticated connection to a HANA database server, you need to provide
connection parameters ([`ConnectParams`]) and
optionally some connection configuration [`ConnectionConfiguration`](crate::ConnectionConfiguration).

The connection parameters define how to establish an authenticated connection (TCP or TLS) to a server.
The second parameter allows influencing the behavior and some performance characteristics of the connection.

[`Connection::new`](../struct.Connection.html#method.new) and
[`Connection::with_configuration`](../struct.Connection.html#method.with_configuration)
take as first parameter an object that implements
[`IntoConnectParams`](../trait.IntoConnectParams.html).

A frequent pattern starts with a URL
(see [`url`](crate::url) for a full description of supported URLs)
and adds user and password programmatically:

```rust,no_run
use hdbconnect::{Connection, ConnectionConfiguration, ConnectParamsBuilder};
# use hdbconnect::HdbResult;
# fn foo() -> HdbResult<()> {
let connection1 = Connection::new(
    ConnectParamsBuilder::from("hdbsqls://myhdb:30715?use_mozillas_root_certificates")?
        .with_dbuser("myName")
        .with_password("mySecret")
)?;

// with non-default configuration:
let connection2 = Connection::with_configuration(
    ConnectParamsBuilder::from("hdbsqls://myhdb:30715?use_mozillas_root_certificates")?
        .with_dbuser("myName")
        .with_password("mySecret"),
    &ConnectionConfiguration::default()
        .with_auto_commit(false)
        .with_read_timeout(Some(std::time::Duration::from_secs(300)))
)?;
# Ok(())
# }
```

## Queries and other database calls

### Generic method: `Connection::statement()` and `HdbResponse`

The most generic way to fire SQL statements without preparation is using
[`Connection`]`::`[`statement()`].
This generic method can handle very different kinds of calls
(SQL queries, DML, procedure calls),
and thus has, on the downside, the most generic OK return type, [`HdbResponse`].

```rust,no_run
# use hdbconnect::{Connection, HdbResult, IntoConnectParams};
# fn foo() -> HdbResult<()> {
# let mut connection = Connection::new("...")?;
let query = "SELECT foo FROM bar";
# #[allow(unused_variables)]
let response = connection.statement(query)?; // HdbResponse
# Ok(())
# }
```

[`HdbResponse`] covers all possible
return values you can get from the database. You thus have to analyze it to understand the
concrete response to your call. (Or you use the respective short-cut method that
fits to your statement, see below).

```rust,no_run
# use hdbconnect::{Connection, HdbResult, IntoConnectParams};
# fn foo() -> HdbResult<()> {
# let mut connection = Connection::new("...")?;
# let query = "SELECT foo FROM bar";
let response = connection.statement(query)?; // HdbResponse
# #[allow(unused_variables)]
let result_set = response.into_result_set()?; // ResultSet
# Ok(())
# }
```

You can do the same of course with [`HdbResponse`]s obtained from the execution
of prepared statements.

### More specific methods with more convenient return values

In many cases it will be more appropriate and convenient to send your database command
with one of the more specialized methods

- [`Connection`]`::`[`query()`]`// HdbResult<ResultSet>`
- [`Connection`]`::`[`dml()`]`// HdbResult<usize>`
- [`Connection`]`::`[`exec()`]`// HdbResult<()>`

which convert the database response directly into a simpler result type:

```rust,no_run
# use hdbconnect::{Connection, HdbResult, IntoConnectParams};
# fn foo() -> HdbResult<()> {
# let mut connection = Connection::new("...")?;
let qry = "SELECT foo FROM bar";
let result_set = connection.query(qry)?; // ResultSet
# Ok(())
# }
```

### Prepared statements

With prepared statements you can use parameters in a database statement, and provide one or
more sets of these parameters in separate API calls before executing the statement.
A parameter set is provided as a reference to a rust value that implements
`serde`'s `Serialize`,
and the serialized field structure must be convertible into the expected parameter value types.

Using a prepared statement could look like this:

```rust,no_run
# #[macro_use] extern crate serde;
# use serde::Serialize;
# use hdbconnect::{Connection, HdbResult, IntoConnectParams};
# fn foo() -> HdbResult<()> {
# let mut connection = Connection::new("...")?;
#[derive(Serialize)]
struct Values{
   s: &'static str,
   i: i32,
};
let v1 = Values{s: "foo", i:45};
let v2 = Values{s: "bar", i:46};

let mut stmt = connection.prepare("insert into COUNTERS (S_KEY, I_VALUE) values(?, ?)")?;
stmt.add_batch(&v1)?;
stmt.add_batch(&v2)?;
stmt.execute_batch()?;
# Ok(())
# }
```

Or like this:

```rust,no_run
# use hdbconnect::{Connection, HdbResult, IntoConnectParams};
# fn foo() -> HdbResult<()> {
# let mut connection = Connection::new("...")?;
let mut stmt = connection.prepare("select NAME, CITY from PEOPLE where iq > ? and age > ?")?;
stmt.add_batch(&(100_u8, 45_i32))?;
let result_set = stmt.execute_batch()?.into_result_set()?;
# Ok(())
# }
```

## Iterating over rows

When iterating over the rows, the result set will implicitly fetch all outstanding rows from the server.

```rust,no_run
# use hdbconnect::{Connection, HdbResult, IntoConnectParams};
# fn foo() -> HdbResult<()> {
# let mut connection = Connection::new("...")?;
# let qry = "";
# let result_set = connection.query(qry)?;
for row in result_set {
    let row = row?;
    // now you have a real row
}
# Ok(())
# }
```

Such a streaming-like behavior is especially appropriate for large result sets.

Iterating over the rows allows writing complex evaluations in an efficient and scalable manner.

```ignore
let key_figure = result_set.map(|r|{r.unwrap()}).map(...).fold(...).collect(...);
```

## Result set evaluation with `try_into()`

While it is possible to iterate over the rows of a result set and then retrieve each value
in each row individually, this driver offers a much more convenient way -
the method `try_into()` allows assigning the result set directly to some appropriate rust data type
of your choice!

`try_into()` is available on [`HdbValue`], [`Row`], and [`ResultSet`],
and is based on the deserialization part of `serde`. It uses return type polymorphism,
which means that you specify explicitly the desired type of the return value, and serde will do
its best to get your data filled in.

### Explicitly evaluating rows

You _can_ retrieve the field values of a row individually, one after the other:

```rust,no_run
# use hdbconnect::{Connection, HdbResult, IntoConnectParams, Row};
# fn foo() -> HdbResult<()> {
# let mut connection = Connection::new("...")?;
# let qry = "";
# let result_set = connection.query(qry)?;
for row in result_set {
    let mut row:Row = row?;
# #[allow(unused_variables)]
    let f1: String = row.next_try_into()?;
# #[allow(unused_variables)]
    let f2: Option<i32> = row.next_try_into()?;
# #[allow(unused_variables)]
    let f3: i32 = row.next_try_into()?;
# #[allow(unused_variables)]
    let f4: chrono::NaiveDateTime = row.next_try_into()?;
}
# Ok(())
# }
```

### Direct conversion of entire rows

With the help of serde you can convert the complete row into a normal rust value, or a tuple,
or a struct with reasonably matching fields.

Since SQL names are often upper-case, while rust field names are usually lower-case,
you'll likely want to use `#[serde(rename = "SQLNAME")]` on structure fields
to bridge this clash of naming conventions, or to map otherwise deviating names:

```rust,no_run
# use serde::Deserialize;
# use hdbconnect::{Connection, HdbResult, IntoConnectParams};
# fn foo() -> HdbResult<()> {
# let mut connection = Connection::new("...")?;
#[derive(Deserialize)]
struct Data {
  #[serde(rename = "NUMBER")]
  number: usize,
  #[serde(rename = "TITLE")]
  description: String,
};

for row in connection.query("SELECT NUMBER,TITLE FROM TABLE_FOO")? {
    let data: Data = row?.try_into()?;
}
# Ok(())
# }
```

### Direct conversion of entire result sets

Even more convenient is the option to convert the complete result set in a single step.
Depending on the concrete numbers of rows and columns, this option supports
a variety of target data structures.

### Matrix-structured result sets

You can always, and __most often want to__, use a `Vec` of a struct or
tuple that matches the fields of the result set.

```rust,no_run
# use serde::Deserialize;
# use hdbconnect::{Connection, HdbResult, IntoConnectParams};
# fn foo() -> HdbResult<()> {
# let mut connection = Connection::new("...")?;
# #[derive(Deserialize)]
# struct MyRow {}
# #[allow(unused_variables)]
# let qry = "...";
let result: Vec<MyRow> = connection.query(qry)?.try_into()?;
# Ok(())
# }
```

## Single-line result sets

If the result set contains only a single line (e.g. because you specified
TOP 1 in your select, or you qualified the full primary key),
then you can also deserialize directly into a plain `MyRow`.

  ```rust,no_run
  # use serde::Deserialize;
  # use hdbconnect::{Connection, HdbResult, IntoConnectParams};
  # fn foo() -> HdbResult<()> {
  # let mut connection = Connection::new("hdbsql://my_user:my_passwd@the_host:2222")?;
  # let qry1 = "SELECT TOP 1 foo FROM bar";
  # #[derive(Deserialize)]
  # struct MyRow {/* ...*/}
  let result: MyRow = connection.query(qry1)?.try_into()?;
  # Ok(())
  # }
  ```

### Single-column result sets

If the result set contains only a single column, then you can choose to
deserialize into a `Vec<field>`,
where `field` is a type that matches the field of the result set.

  ```rust, no_run
# use hdbconnect::{Connection, HdbResult, IntoConnectParams};
# fn foo() -> HdbResult<()> {
# let mut connection = Connection::new("...")?;
# let qry = "";
  let result: Vec<u32> = connection.query(qry)?.try_into()?;
# Ok(())
# }
  ```

### Single-value result sets

If the result set contains only a single value (one row with one column),
then you can also deserialize into a plain `field`:

  ```rust, no_run
# use hdbconnect::{Connection, HdbResult, IntoConnectParams};
# fn foo() -> HdbResult<()> {
# let mut connection = Connection::new("...")?;
# let qry = "";
  let result: u32 = connection.query(qry)?.try_into()?;
# Ok(())
# }
  ```

## Deserialization of field values

The deserialization of individual values provides flexibility without data loss:

- You can e.g. convert values from a nullable column into a plain field,
  provided that no NULL values are given in the result set.

- Vice versa, you can use an `Option<field>` as target structure,
  even if the column is marked as NOT NULL.

- Source and target integer types can differ from each other,
  as long as the concrete values can be assigned without loss.

- You can convert numeric values on-the-fly into default String representations.

## Binary Values

So far, specialization support is not yet in rust stable. Without that, you  have to use
[`serde_bytes::Bytes`] and [`serde_bytes::ByteBuf`]
as lean wrappers around `&[u8]` and `Vec<u8>`
to serialize into or deserialize from binary database types.

```ignore
let raw_data: Vec<u8> = ...;
insert_stmt.add_batch(&(Bytes::new(&*raw_data)))?;
```

```rust, no_run
# use hdbconnect::{Connection, ResultSet, HdbResult, IntoConnectParams};
# fn foo() -> HdbResult<()> {
# let qry = "";
# let mut connection = Connection::new("...")?;
# let result_set: ResultSet = connection.query(qry)?;
let bindata: serde_bytes::ByteBuf = result_set.try_into()?; // single binary field
let first_byte = bindata[0];
# Ok(())
# }
```

## LOBs

Character and binary LOBs can be treated like "normal" String and binary data, i.e.
you can convert them with the methods described above into String or [`serde_bytes::ByteBuf`] values
(see [`serde_bytes`] for serde's specialties regarding bytes).

### Streaming LOBs to the database

Avoid materializing the complete "Large Object" by handing over a reader that provides the data.
An internal buffer will be filled by reading from the reader.
If the internal buffer has reached the value of the connection's lob write size,
data are sent to the database and the buffer can be filled anew.

```rust, no_run
  use std::sync::{Arc, Mutex};
  # use hdbconnect::HdbValue;
  # struct DummyReader;
  # impl std::io::Read for DummyReader{
  #     fn read(&mut self, _: &mut [u8]) -> Result<usize, std::io::Error> { unimplemented!() }
  # }
  # let reader: DummyReader = unimplemented!();
  # let insert_stmt: hdbconnect::PreparedStatement = unimplemented!();
  let am_reader = Arc::new(Mutex::new(reader));
  insert_stmt.execute_row(vec![
      HdbValue::STR("streaming2"),
      HdbValue::SYNC_LOBSTREAM(Some(am_reader)),
  ]).unwrap();

```

### Streaming LOBs from the database

Avoid materializing the complete "Large Object" by converting the `HdbValue`
into the corresponding Lob object (one of `hdbconnect::{BLob, CLob, NCLob}`)
and reading from it incrementally.
When the internal buffer is empty, new data will be read from the database
in chunks of the connection's lob read size.

In this example the [`NCLob`] will, while being read,
continuously fetch more data from the database until it is completely transferred:

```rust, no_run
use hdbconnect::{Connection, HdbResult, IntoConnectParams, ResultSet};
use hdbconnect::types::NCLob;
# fn foo() -> HdbResult<()> {
# let query = "";
# let mut connection = Connection::new("...")?;
# let mut result_set: ResultSet = connection.query(query)?;
# let mut writer: Vec<u8> = vec![];
let mut nclob: NCLob = result_set.into_single_value()?.try_into_nclob()?;
std::io::copy(&mut nclob, &mut writer)?;
# Ok(())
# }
```

[`Connection`]: crate::Connection
[`statement()`]: crate::Connection::statement
[`query()`]: crate::Connection::query
[`dml()`]: crate::Connection::dml
[`exec()`]: crate::Connection::exec
[`ConnectParams`]: crate::ConnectParams
[`HdbValue`]: crate::HdbValue
[`HdbResponse`]: crate::HdbResponse
[`NCLob`]: crate::types::NCLob
[`Row`]: crate::Row
[`ResultSet`]: crate::ResultSet
[`serde_bytes`]: https://docs.serde.rs/serde_bytes/
[`serde_bytes::Bytes`]: https://docs.serde.rs/serde_bytes/struct.Bytes.html
[`serde_bytes::ByteBuf`]: https://docs.serde.rs/serde_bytes/struct.ByteBuf.html
