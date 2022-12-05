//! Code examples.
//!
//! # 1. Database connections
//!
//! Establish an authenticated connection to the database server
//! (see also [`ConnectParams`]):
//!
//! ```rust,no_run
//! use hdbconnect::{Connection, IntoConnectParams};
//! # use hdbconnect::HdbResult;
//! # fn foo() -> HdbResult<()> {
//! // connect directly to a database:
//! let mut connection1 = Connection::new("hdbsql://my_user:my_passwd@the_host:30815")?;
//!
//! // connect to the port of the system db and let the db redirect to the correct host and port:
//! let mut connection2 = Connection::new("hdbsql://my_user:my_passwd@the_host:30813?db=MEI")?;
//! # Ok(())
//! # }
//! ```
//!
//! # 2. Queries and other database calls
//!
//! ## 2.1 Generic method: `Connection::statement()` and `HdbResponse`
//!
//! The most generic way to fire SQL statements without preparation is using
//! [`Connection`]`::`[`statement()`].
//! This generic method can handle very different kinds of calls
//! (SQL queries, DML, procedure calls),
//! and thus has the most generic OK return type, [`HdbResponse`].
//!
//! ```rust,no_run
//! # use hdbconnect::{Connection, HdbResult, IntoConnectParams};
//! # fn foo() -> HdbResult<()> {
//! # let mut connection = Connection::new("...")?;
//! let query = "SELECT foo FROM bar";
//! # #[allow(unused_variables)]
//! let response = connection.statement(query)?; // HdbResponse
//! # Ok(())
//! # }
//! ```
//![`HdbResponse`] covers all possible
//! return values we can get from the database.
//! You thus have to analyze it to get to the
//! concrete response to your call. Or you use the respective short-cut method that
//! fits to your statement.
//!
//! ```rust,no_run
//! # use hdbconnect::{Connection, HdbResult, IntoConnectParams};
//! # fn foo() -> HdbResult<()> {
//! # let mut connection = Connection::new("...")?;
//! # let query = "SELECT foo FROM bar";
//! let response = connection.statement(query)?; // HdbResponse
//! # #[allow(unused_variables)]
//! let resultset = response.into_resultset()?; // ResultSet
//! # Ok(())
//! # }
//! ```
//!
//! You can do the same of course with [`HdbResponse`]s obtained from the execution
//! of prepared statements.
//!
//! ## 2.2 More specific methods with tailored return values
//!
//! In many cases it will be more appropriate and convenient to send your database command
//! with one of the more specialized methods
//!
//! * [`Connection`]`::`[`query()`]` // HdbResult<ResultSet>`
//! * [`Connection`]`::`[`dml()`]`   // HdbResult<usize>`
//! * [`Connection`]`::`[`exec()`]`  // HdbResult<()>`
//!
//! which convert the database response directly into a simpler result type:
//!
//! ```rust,no_run
//! # use hdbconnect::{Connection, HdbResult, IntoConnectParams};
//! # fn foo() -> HdbResult<()> {
//! # let mut connection = Connection::new("...")?;
//! let qry = "SELECT foo FROM bar";
//! let resultset = connection.query(qry)?; // ResultSet
//! # Ok(())
//! # }
//! ```
//!
//! ## 2.3 Prepared statements
//!
//! With prepared statements you can use parameters in a database statement, and provide one or
//! more sets of these parameters in separate API calls before executing the statement.
//! A parameter set is provided as a reference to a rust value that implements
//! `serde`'s `Serialize`,
//! and the serialized field structure must be convertible into the expected parameter value types.
//!
//! Using a prepared statement could look like this:
//!
//! ```rust,no_run
//! # #[macro_use] extern crate serde;
//! # use serde::Serialize;
//! # use hdbconnect::{Connection, HdbResult, IntoConnectParams};
//! # fn foo() -> HdbResult<()> {
//! # let mut connection = Connection::new("...")?;
//! #[derive(Serialize)]
//! struct Values{
//!    s: &'static str,
//!    i: i32,
//! };
//! let v1 = Values{s: "foo", i:45};
//! let v2 = Values{s: "bar", i:46};
//!
//! let mut stmt = connection.prepare("insert into COUNTERS (S_KEY, I_VALUE) values(?, ?)")?;
//! stmt.add_batch(&v1)?;
//! stmt.add_batch(&v2)?;
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
//! # let mut connection = Connection::new("...")?;
//! let mut stmt = connection.prepare("select NAME, CITY from PEOPLE where iq > ? and age > ?")?;
//! stmt.add_batch(&(100_u8, 45_i32))?;
//! let resultset = stmt.execute_batch()?.into_resultset()?;
//! # Ok(())
//! # }
//! ```
//!
//! # 3. Result set evaluation
//!
//! Some of the following examples use the method `try_into()`, on an individual [`HdbValue`],
//! a [`Row`], or a [`ResultSet`].
//! These methods are based on the deserialization part of `serde` and use return type polymorphism,
//! which means that you need to specify explicitly the desired type of the return value.
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
//! # let mut connection = Connection::new("...")?;
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
//! # fn foo() -> HdbResult<()> {
//! # let mut connection = Connection::new("...")?;
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
//! # use serde::Deserialize;
//! # use hdbconnect::{Connection, HdbResult, IntoConnectParams};
//! # fn foo() -> HdbResult<()> {
//! # let mut connection = Connection::new("...")?;
//! # let qry = "";
//! # let resultset = connection.query(qry)?;
//! #[derive(Deserialize)]
//! struct TestData {/* ...*/};
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
//! # use serde::Deserialize;
//! # use hdbconnect::{Connection, HdbResult, IntoConnectParams};
//! # fn foo() -> HdbResult<()> {
//! # let mut connection = Connection::new("...")?;
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
//!   # use serde::Deserialize;
//!   # use hdbconnect::{Connection, HdbResult, IntoConnectParams};
//!   # fn foo() -> HdbResult<()> {
//!   # let mut connection = Connection::new("hdbsql://my_user:my_passwd@the_host:2222")?;
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
//! # let mut connection = Connection::new("...")?;
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
//! # let mut connection = Connection::new("...")?;
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
//! [`serde_bytes::Bytes`] and [`serde_bytes::ByteBuf`]
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
//! # let qry = "";
//! # let mut connection = Connection::new("...")?;
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
//! you can convert them with the methods described above into [`serde_bytes::ByteBuf`]
//! or String values (see [`serde_bytes`] for serde's specialties regarding bytes).
//!
//! If necessary, you can easily avoid materializing the complete "Large Object",
//! and stream it e.g. into a writer. For doing so, you convert the value into one of
//! `hdbconnect::{BLob, CLob, NCLob}`.
//!
//! In this example the [`NCLob`] will, while being read by `std::io::copy()`,
//! continuously fetch more data from the database until it is completely transferred:
//!
//! ```rust, no_run
//! use hdbconnect::{Connection, HdbResult, IntoConnectParams, ResultSet};
//! use hdbconnect::types::NCLob;
//! # fn foo() -> HdbResult<()> {
//! # let query = "";
//! # let mut connection = Connection::new("...")?;
//! # let mut resultset: ResultSet = connection.query(query)?;
//! # let mut writer: Vec<u8> = vec![];
//! let mut nclob: NCLob = resultset.into_single_row()?.into_single_value()?.try_into_nclob()?;
//! std::io::copy(&mut nclob, &mut writer)?;
//! # Ok(())
//! # }
//! ```
//!
//! [`Connection`]: crate::Connection
//! [`statement()`]: crate::Connection::statement
//! [`query()`]: crate::Connection::query
//! [`dml()`]: crate::Connection::dml
//! [`exec()`]: crate::Connection::exec
//! [`ConnectParams`]: crate::ConnectParams
//! [`HdbValue`]: crate::HdbValue
//! [`HdbResponse`]: crate::HdbResponse
//! [`NCLob`]: crate::types::NCLob
//! [`Row`]: crate::Row
//! [`ResultSet`]: crate::ResultSet
//! [`serde_bytes`]: https://docs.serde.rs/serde_bytes/
//! [`serde_bytes::Bytes`]: https://docs.serde.rs/serde_bytes/struct.Bytes.html
//! [`serde_bytes::ByteBuf`]: https://docs.serde.rs/serde_bytes/struct.ByteBuf.html
