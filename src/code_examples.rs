//!
//! # 1. Database connections
//!
//! Establish an authenticated connection to the database server
//! (see also [`ConnectParams`](../struct.ConnectParams.html)):
//!
//! ```rust,no_run
//! use hdbconnect::{Connection, IntoConnectParams};
//! # use hdbconnect::HdbResult;
//! # fn foo() -> HdbResult<()> {
//! let params = "hdbsql://my_user:my_passwd@the_host:2222".into_connect_params()?;
//! let mut connection = Connection::new(params)?;
//! # Ok(())
//! # }
//! ```
//!
//! # 2. Queries and other database calls
//!
//! ## 2.1 Generic method: Connection.statement() and HdbResponse()
//!
//! The most generic way to fire SQL statements without preparation is using
//! `Connection::statement()`.
//! This generic method can handle very different kinds of calls
//! (SQL queries, DML, procedure calls),
//! and thus has the most complex return type, [`HdbResponse`](struct.HdbResponse.html).
//!
//! ```rust,no_run
//! # use hdbconnect::{Connection, HdbResult, IntoConnectParams};
//! # fn foo() -> HdbResult<()> {
//! # let params = "".into_connect_params()?;
//! # let mut connection = Connection::new(params)?;
//! let query = "SELECT foo FROM bar";
//! # #[allow(unused_variables)]
//! let response = connection.statement(query)?; // HdbResponse
//! # Ok(())
//! # }
//! ```
//![`HdbResponse`](struct.HdbResponse.html) covers all possible
//! return values we can get from the database.
//! You thus have to analyze it to get to the
//! concrete response to your call. Or you use the respective short-cut method that
//! fits to your statement.
//!
//! ```rust,no_run
//! # use hdbconnect::{Connection, HdbResult, IntoConnectParams};
//! # fn foo() -> HdbResult<()> {
//! # let params = "".into_connect_params()?;
//! # let mut connection = Connection::new(params)?;
//! # let query = "SELECT foo FROM bar";
//! let response = connection.statement(query)?; // HdbResponse
//! # #[allow(unused_variables)]
//! let resultset = response.into_resultset()?; // ResultSet
//! # Ok(())
//! # }
//! ```
//!
//! You can do the same of course with `HdbResponse`s obtained from the execution
//! of prepared statements.
//!
//! ## 2.2 More specific methods with tailored return values
//!
//! In many cases it will be more appropriate and convenient to send your database command
//! with one of the more specialized methods
//!
//! * `connection.query(...) // ResultSet`
//! * `connection.dml(...)   // usize`
//! * `connection.exec(...)  // ()`
//!
//! which convert the database response directly into a simpler result type:
//!
//! ```rust,no_run
//! # use hdbconnect::{Connection, HdbResult, IntoConnectParams};
//! # fn foo() -> HdbResult<()> {
//! # let params = "".into_connect_params()?;
//! # let mut connection = Connection::new(params)?;
//! let qry = "SELECT foo FROM bar";
//! let resultset = connection.query(qry)?; // ResultSet
//! # Ok(())
//! # }
//! ```
//!
//! ## 2.3 Prepared statements
//!
//! With prepared statements, the code will look like this:
//!
//! ```rust,no_run
//! # use hdbconnect::{Connection, HdbResult, IntoConnectParams};
//! # fn foo() -> HdbResult<()> {
//! # let params = "".into_connect_params()?;
//! # let mut connection = Connection::new(params)?;
//! let stmt_str = "insert into TEST_PREPARE (F_STRING, F_INTEGER) values(?, ?)";
//! let mut stmt = connection.prepare(stmt_str)?;
//! stmt.add_batch(&("foo", 45_i32))?;
//! stmt.add_batch(&("bar", 46_i32))?;
//! stmt.execute_batch()?;
//! # Ok(())
//! # }
//! ```
//!
//! Or like this:
//!
//! ```rust,no_run
//! # use hdbconnect::{Connection, HdbResult, IntoConnectParams};
//! # fn foo() -> HdbResult<()> {
//! # let params = "".into_connect_params()?;
//! # let mut connection = Connection::new(params)?;
//! let stmt_str = "select NAME, CITY from TEST_TABLE where age > ?";
//! let mut stmt = connection.prepare(stmt_str)?;
//! stmt.add_batch(&(45_i32))?;
//! let resultset = stmt.execute_batch()?.into_resultset()?;
//! # Ok(())
//! # }
//! ```
//!
//! # 3. Result set evaluation
//!
//! Some of the following examples use a method `try_into()`, on an individual `HdbValue`,
//! a `Row`, or a `ResultSet`.
//! These methods use return type polymorphism (based on `serde`), which means that you
//! need to specify explicitly the desired type of the return value.
//!
//! # 3.1 Iterating over rows
//!
//! Evaluating a result set by iterating over the rows explicitly is possible, of course.
//! Note that the row iterator returns `HdbResult<Row>`, not `Row`,
//! because the result set might need to fetch more rows lazily from the database, which can fail.
//!
//! ```rust,no_run
//! # use hdbconnect::{Connection, HdbResult, IntoConnectParams};
//! # fn foo() -> HdbResult<()> {
//! # let params = "".into_connect_params()?;
//! # let mut connection = Connection::new(params)?;
//! # let qry = "";
//! # let resultset = connection.query(qry)?;
//! for row in resultset {
//!     let row = row?;
//!     // now you have a real row
//! }
//! # Ok(())
//! # }
//! ```
//!
//! Such a streaming-like behavior is especially appropriate for large result sets.
//! Iterating over the rows, while they are fetched on-demand from the server in smaller portions,
//! makes it easy to write complex evaluations in an efficient and scalable manner.
//!
//! ```ignore
//! let key_figure = resultset.map(|r|{r.unwrap()}).map(...).fold(...).collect(...);
//! ```
//!
//! # 3.2 Explicitly evaluating a single row
//!
//! You _can_ retrieve the field values of a row individually, one after the other:
//!
//! ```rust,no_run
//! # use hdbconnect::{Connection, HdbResult, IntoConnectParams, Row};
//! # fn main() { }
//! # fn foo() -> HdbResult<()> {
//! # let params = "".into_connect_params()?;
//! # let mut connection = Connection::new(params)?;
//! # let qry = "";
//! # let resultset = connection.query(qry)?;
//! for row in resultset {
//!     let mut row:Row = row?;
//! # #[allow(unused_variables)]
//!     let f1: String = row.next_try_into()?;
//! # #[allow(unused_variables)]
//!     let f2: Option<i32> = row.next_try_into()?;
//! # #[allow(unused_variables)]
//!     let f3: i32 = row.next_try_into()?;
//! # #[allow(unused_variables)]
//!     let f4: chrono::NaiveDateTime = row.next_try_into()?;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # 3.3 Direct conversion of entire rows
//!
//! A usually more convenient way is to convert the complete row into a normal rust value
//! or tuple or struct:
//!
//! ```rust,no_run
//! # use serde_derive::Deserialize;
//! # use hdbconnect::{Connection, HdbResult, IntoConnectParams};
//! # fn main() { }
//! # fn foo() -> HdbResult<()> {
//! # let params = "".into_connect_params()?;
//! # let mut connection = Connection::new(params)?;
//! # let qry = "";
//! # let resultset = connection.query(qry)?;
//! #[derive(Deserialize)]
//! struct TestData {/* ...*/}
//! let qry = "select * from TEST_RESULTSET";
//! for row in connection.query(qry)? {
//!     let td: TestData = row?.try_into()?;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # 3.4 Direct conversion of entire result sets
//!
//! Even more convenient is the option to convert the complete result set in a single step.
//! Depending on the concrete numbers of rows and columns, this option supports
//! a variety of target data structures.
//!
//! # 3.4.1 Matrix-structured result sets
//!
//! You can always, and __most often want to__, use a `Vec` of a struct or
//! tuple that matches the fields of the result set.
//!
//! ```rust,no_run
//! # use serde_derive::Deserialize;
//! # use hdbconnect::{Connection, HdbResult, IntoConnectParams};
//! # fn main() { }
//! # fn foo() -> HdbResult<()> {
//! # let params = "".into_connect_params()?;
//! # let mut connection = Connection::new(params)?;
//! # let qry = "";
//! #[derive(Deserialize)]
//! struct MyRow {/* ...*/}
//!
//! # #[allow(unused_variables)]
//! let result: Vec<MyRow> = connection.query(qry)?.try_into()?;
//! # Ok(())
//! # }
//! ```
//!
//! # 3.4.2 Single-line result sets
//!
//! If the result set contains only a single line (e.g. because you specified
//! TOP 1 in your select, or you qualified the full primary key),
//! then you can also deserialize directly into a plain `MyRow`.
//!
//!   ```rust,no_run
//!   # use serde_derive::Deserialize;
//!   # use hdbconnect::{Connection, HdbResult, IntoConnectParams};
//!   # fn main() { }
//!   # fn foo() -> HdbResult<()> {
//!   # let params = "hdbsql://my_user:my_passwd@the_host:2222".into_connect_params()?;
//!   # let mut connection = Connection::new(params)?;
//!   # let qry = "SELECT foo FROM bar";
//!   # #[derive(Deserialize)]
//!   # struct MyRow {/* ...*/}
//!   let result: MyRow = connection.query(qry)?.try_into()?;
//!   # Ok(())
//!   # }
//!   ```
//!
//! # 3.4.3 Single-column result sets
//!
//! If the result set contains only a single column, then you can choose to
//! deserialize into a `Vec<field>`,
//! where `field` is a type that matches the field of the result set.
//!
//!   ```rust, no_run
//! # use hdbconnect::{Connection, HdbResult, IntoConnectParams};
//! # fn foo() -> HdbResult<()> {
//! # let params = "".into_connect_params()?;
//! # let mut connection = Connection::new(params)?;
//! # let qry = "";
//!   let result: Vec<u32> = connection.query(qry)?.try_into()?;
//! # Ok(())
//! # }
//!   ```
//!
//! # 3.4.4 Single-value result sets
//!
//! If the result set contains only a single value (one row with one column),
//! then you can also deserialize into a plain `field`:
//!
//!   ```rust, no_run
//! # use hdbconnect::{Connection, HdbResult, IntoConnectParams};
//! # fn foo() -> HdbResult<()> {
//! # let params = "".into_connect_params()?;
//! # let mut connection = Connection::new(params)?;
//! # let qry = "";
//!   let result: u32 = connection.query(qry)?.try_into()?;
//! # Ok(())
//! # }
//!   ```
//!
//! # 4. Deserialization of field values
//!
//! The deserialization of individual values provides flexibility without data loss:
//!
//! * You can e.g. convert values from a nullable column into a plain field,
//!   provided that no NULL values are given in the result set.
//!
//! * Vice versa, you can use an `Option<field>` as target structure,
//!   even if the column is marked as NOT NULL.
//!
//! * Source and target integer types can differ from each other,
//!   as long as the concrete values can be assigned without loss.
//!
//! * You can convert numeric values on-the-fly into default String representations.
//!
//! You should use this flexibility with some care though, or you will face errors
//! when the data violates the boundaries of the target values.
//!
//!
//! # 5. Binary Values
//!
//! So far, specialization support is not yet in rust stable. Without that, you  have to use
//! [`serde_bytes::Bytes`](https://docs.serde.rs/serde_bytes/struct.Bytes.html) and
//! [`serde_bytes::ByteBuf`](https://docs.serde.rs/serde_bytes/struct.ByteBuf.html)
//! as lean wrappers around `&[u8]` and `Vec<u8>`
//! to serialize into or deserialize from binary database types.
//!
//! ```ignore
//! let raw_data: Vec<u8> = ...;
//! insert_stmt.add_batch(&(Bytes::new(&*raw_data)))?;
//! ```
//!
//!
//! ```rust, no_run
//! # use hdbconnect::{Connection, ResultSet, HdbResult, IntoConnectParams};
//! # fn foo() -> HdbResult<()> {
//! # let params = "".into_connect_params()?;
//! # let qry = "";
//! # let mut connection = Connection::new(params)?;
//! # let resultset: ResultSet = connection.query(qry)?;
//! let bindata: serde_bytes::ByteBuf = resultset.try_into()?; // single binary field
//! let first_byte = bindata[0];
//! # Ok(())
//! # }
//! ```
//!
//!
//! # 6. LOBs
//! Binary and Character LOBs can be treated like "normal" binary and String data, i.e.
//! you can convert them with the methods described above into `ByteBuf` or String values
//! (see [serde_bytes](https://docs.serde.rs/serde_bytes/) for serde's specialties regarding bytes).
//!
//! If necessary, you can easily avoid materializing the complete "Large Object",
//! and stream it e.g. into a writer. For doing so, you convert the value into one of
//! `hdbconnect::{BLob, CLob, NCLob}`.
//!
//! In this example the [`NCLob`](types/struct.NCLob.html) will,
//! while being read by `std::io::copy()`,
//! continuously fetch more data from the database until it is completely transferred:
//!
//! ```rust, no_run
//! use hdbconnect::{Connection, HdbResult, IntoConnectParams, ResultSet};
//! use hdbconnect::types::NCLob;
//! # fn foo() -> HdbResult<()> {
//! # let params = "".into_connect_params()?;
//! # let query = "";
//! # let mut connection = Connection::new(params)?;
//! # let mut resultset: ResultSet = connection.query(query)?;
//! # let mut writer: Vec<u8> = vec![];
//! let mut nclob: NCLob = resultset.into_single_row()?.into_single_value()?.try_into_nclob()?;
//! std::io::copy(&mut nclob, &mut writer)?;
//! # Ok(())
//! # }
//! ```
//!
