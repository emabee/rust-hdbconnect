//! Here are some code examples for the usage of this database driver.
//!
//! <b>1. Database connections</b>
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
//! <b>2. Queries and other database calls</b>
//!
//! The most generic way to fire SQL statements without preparation is using
//! `Connection::statement()`.
//! This generic method can handle very different kinds of calls (queries, dml, procedures),
//! and thus has the most complex return type, `HdbResponse`.
//!
//! ```rust,no_run
//! # use hdbconnect::{Connection, HdbResult, IntoConnectParams};
//! # fn foo() -> HdbResult<()> {
//! # let params = "hdbsql://my_user:my_passwd@the_host:2222".into_connect_params()?;
//! # let mut connection = Connection::new(params)?;
//! let query = "SELECT foo FROM bar";
//! # #[allow(unused_variables)]
//! let response = connection.statement(query)?; // HdbResponse
//! # Ok(())
//! # }
//! ```
//!
//! `HdbResponse` is a nested enum which covers all possible
//! return values we can get from the database.
//! You thus have to analyze it to come to the
//! concrete response relevant for your call.
//! You can do this either explicitly using `match` etc or with the
//! adequate short-cut method, e.g.:
//!
//! ```rust,no_run
//! # use hdbconnect::{Connection, HdbResult, IntoConnectParams};
//! # fn foo() -> HdbResult<()> {
//! # let params = "hdbsql://my_user:my_passwd@the_host:2222".into_connect_params()?;
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
//! In many cases it will be more appropriate and convenient to use one of the
//! specialized methods
//!
//! * `connection.query(...) // ResultSet`
//! * `connection.dml(...)   // usize`
//! * `connection.exec(...)  // ()`
//!
//! where each has an adequate simple result type:
//!
//! ```rust,no_run
//! # use hdbconnect::{Connection, HdbResult, IntoConnectParams};
//! # fn foo() -> HdbResult<()> {
//! # let params = "hdbsql://my_user:my_passwd@the_host:2222".into_connect_params()?;
//! # let mut connection = Connection::new(params)?;
//! let my_statement = "SELECT foo FROM bar";
//! let resultset = connection.query(my_statement)?; // ResultSet
//! # Ok(())
//! # }
//! ```
//!
//! With prepared statements, the code will look like this:
//!
//! ```rust,no_run
//! # use hdbconnect::{Connection, HdbResult, IntoConnectParams};
//! # fn foo() -> HdbResult<()> {
//! # let params = "hdbsql://my_user:my_passwd@the_host:2222".into_connect_params()?;
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
//! # let params = "hdbsql://my_user:my_passwd@the_host:2222".into_connect_params()?;
//! # let mut connection = Connection::new(params)?;
//! let stmt_str = "select NAME, CITY from TEST_TABLE where age > ?";
//! let mut stmt = connection.prepare(stmt_str)?;
//! stmt.add_batch(&(45_i32))?;
//! let resultset = stmt.execute_batch()?;
//! # Ok(())
//! # }
//! ```
//!
//! <b>3. Resultset evaluation</b>
//!
//! Evaluating a resultset by iterating over the rows explicitly is possible, of course.
//! Note that the row iterator returns `HdbResult<Row>`, not `Row`,
//! because the resultset might need to fetch more rows lazily from the server, which can fail.
//!
//! ```rust,no_run
//! # use hdbconnect::{Connection, HdbResult, IntoConnectParams};
//! # fn foo() -> HdbResult<()> {
//! # let params = "hdbsql://my_user:my_passwd@the_host:2222".into_connect_params()?;
//! # let mut connection = Connection::new(params)?;
//! # let my_statement = "SELECT foo FROM bar";
//! # let resultset = connection.query(my_statement)?; // ResultSet
//! for row in resultset {
//!     let row = row?;
//!     // now you have a real row
//! }
//! # Ok(())
//! # }
//! ```
//!
//! Such a streaming-like behavior is especially appropriate for large resultsets.
//! Iterating over the rows, while they are fetched on-demand from the server in smaller portions,
//! makes it easy to write complex evaluations in an efficient and scalable manner.
//!
//! ```ignore
//! let key_figure = resultset.into_iter()?.map(|r|{r?}).filter(...).fold(...);
//! ```
//!
//! You _can_ retrieve the field values of a row  individually, in arbitrary order.
//! `hdbconnect::Row` provides for this a single
//! method that is generalized by its return value,
//! so you need to specify the target type explicitly:
//!
//! ```rust,no_run
//! # extern crate chrono;
//! # extern crate hdbconnect;
//! # use hdbconnect::{Connection, HdbResult, IntoConnectParams, Row};
//! use chrono::NaiveDateTime;
//! # fn main() { }
//! # fn foo() -> HdbResult<()> {
//! # let params = "hdbsql://my_user:my_passwd@the_host:2222".into_connect_params()?;
//! # let mut connection = Connection::new(params)?;
//! # let my_statement = "SELECT foo FROM bar";
//! # let resultset = connection.query(my_statement)?; // ResultSet
//! for row in resultset {
//!     let row:Row = row?;
//! # #[allow(unused_variables)]
//!     let f4: NaiveDateTime = row.field_as(3)?;
//! # #[allow(unused_variables)]
//!     let f1: String = row.field_as(0)?;
//! # #[allow(unused_variables)]
//!     let f3: i32 = row.field_as(2)?;
//! # #[allow(unused_variables)]
//!     let f2: Option<i32> = row.field_as(1)?;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! A usually more convenient way is to convert the complete row into a normal rust value
//! or tuple or struct:
//!
//! ```rust,no_run
//! # #[macro_use]
//! # extern crate serde_derive;
//! # extern crate hdbconnect;
//! # use hdbconnect::{Connection, HdbResult, IntoConnectParams};
//! # fn main() { }
//! # fn foo() -> HdbResult<()> {
//! # let params = "hdbsql://my_user:my_passwd@the_host:2222".into_connect_params()?;
//! # let mut connection = Connection::new(params)?;
//! # let my_statement = "SELECT foo FROM bar";
//! # let resultset = connection.query(my_statement)?; // ResultSet
//! #[derive(Deserialize)]
//! struct TestData {/* ...*/}
//!
//! for row in connection.query("select * from TEST_RESULTSET")? {
//!     let td: TestData = row?.try_into()?;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! As hdbconnect uses serde for this conversion, you need to
//! specify the type of your target variable explicitly.
//!
//! Sometimes even more convenient is the option to convert the complete resultset in a single step.
//! This option supports
//! a variety of target data structures, depending on the concrete numbers of rows and columns.
//!
//! * You can always use a <code>Vec</code> of a struct or tuple that matches
//!   the fields of the resultset.
//!
//! ```rust,no_run
//! # #[macro_use]
//! # extern crate serde_derive;
//! # extern crate hdbconnect;
//! # use hdbconnect::{Connection, HdbResult, IntoConnectParams};
//! # fn main() { }
//! # fn foo() -> HdbResult<()> {
//! # let params = "hdbsql://my_user:my_passwd@the_host:2222".into_connect_params()?;
//! # let mut connection = Connection::new(params)?;
//! # let my_statement = "SELECT foo FROM bar";
//! # let resultset = connection.query(my_statement)?; // ResultSet
//! #[derive(Deserialize)]
//! struct MyRow {/* ...*/}
//!
//! let result: Vec<MyRow> = resultset.try_into()?;
//! # Ok(())
//! # }
//! ```
//!
//! * If the resultset contains only a single line (e.g. because you specified
//!   TOP 1 in your select, or you qualified the full primary key),
//!   then you can choose to deserialize into a plain <code>`MyRow`</code> directly.
//!
//!   ```rust,no_run
//!   # #[macro_use]
//!   # extern crate serde_derive;
//!   # extern crate hdbconnect;
//!   # use hdbconnect::{Connection, HdbResult, IntoConnectParams};
//!   # fn main() { }
//!   # fn foo() -> HdbResult<()> {
//!   # let params = "hdbsql://my_user:my_passwd@the_host:2222".into_connect_params()?;
//!   # let mut connection = Connection::new(params)?;
//!   # let my_statement = "SELECT foo FROM bar";
//!   # let resultset = connection.query(my_statement)?; // ResultSet
//!   # #[derive(Deserialize)]
//!   # struct MyRow {/* ...*/}
//!   let result: MyRow = resultset.try_into()?;
//!   ```
//!
//! * If the resultset contains only a single column, then you can choose to
//!   deserialize into a <code>Vec&lt;field&gt;</code>,
//!   where <code>field</code> is a type that matches the field of the resultset.
//!   If a plain rust type is used, you don't even need to derive Deserialize:
//!
//!   ```ignore
//!   let result: Vec<u32> = resultset.try_into()?;
//!   ```
//!
//! * If the resultset contains only a single value (one row with one column),
//!   then you can also deserialize into a plain <code>field</code>:
//!
//!   ```ignore
//!   let result: u32 = resultset.try_into()?;
//!   ```
//!
//! <b>4. Deserialization of field values</b>
//!
//! The deserialization of individual values provides flexibility without data loss:
//!
//! * You can e.g. convert values from a nullable column into a plain field,
//!   provided that no NULL values are given in the resultset.
//!
//! * Vice versa, you can use an Option<code>&lt;field&gt;</code> as target structure,
//!   even if the column is marked as NOT NULL.
//!
//! * Source and target integer types can be different from each other,
//!   as long as the concrete values can be assigned without loss.
//!
//! You should use this flexibilty only if you know that the data never violates these boundaries.
