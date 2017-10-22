//! Here are some code examples for the usage of this database driver.
//!
//! <b>1. Database connections</b>
//!
//! Establish an authenticated connection to the database server
//! (see also [`ConnectParams`](../struct.ConnectParams.html)):
//!
//! ```ignore
//! let params: ConnectParams = ...;
//! let mut connection = Connection::new(params)?;
//! ```
//!
//! <b>2. Queries and other database calls</b>
//!
//! The most generic way to fire SQL statements without preparation is using
//! <code>`Connection::statement`()</code>.
//! This generic method can handle very different kinds of calls (queries, dml, procedures),
//! and thus has the most complex return type, <code>`HdbResponse`</code>.
//!
//! ```ignore
//! let query = "SELECT foo FROM bar";
//! let response: HdbResponse = connection.statement(query)?;
//! ```
//!
//! <code>`HdbResponse`</code> is a nested enum which covers all possible
//! return values we can get from the database.
//! You thus have to analyze it to come to the
//! concrete response relevant for your call.
//! You can do this either explicitly using <code>match</code> etc or with the
//! adequate short-cut method, e.g.:
//!
//! ```ignore
//! let resultset: ResultSet = response.as_resultset()?;
//! ```
//!
//! You can do the same of course with <code>`HdbResponse`</code>s obtained from the execution
//! of prepared statements.
//!
//! In many cases it will be more appropriate and convenient to use one of the
//! specialized methods
//!
//! * <code>connection.query(...)</code> -> `ResultSet`
//! * <code>connection.dml(...)</code> -> usize
//! * <code>connection.exec(...)</code> -> ()
//!
//! where each has an adequate simple result type:
//!
//! ```ignore
//! let my_statement = "SELECT foo FROM bar";
//! let resultset = connection.query(my_statement)?; // ResultSet
//! ```
//!
//! With prepared statements, the code will look like this:
//!
//! ```ignore
//! let stmt_str = "insert into TEST_PREPARE (F_STRING, F_INTEGER) values(?, ?)";
//! let mut stmt = connection.prepare(stmt_str)?;
//! stmt.add_batch(&("foo", 45_i32))?;
//! stmt.add_batch(&("bar", 46_i32))?;
//! stmt.execute_batch()?;
//! ```
//!
//! Or like this:
//!
//! ```ignore
//! let stmt_str = "select NAME, CITY from TEST_TABLE where age > ?";
//! let mut stmt = connection.prepare(stmt_str)?;
//! stmt.add_batch(&(45_i32))?;
//! let resultset = stmt.execute_batch()?;
//! ```
//!
//! <b>3. Resultset evaluation</b>
//!
//! Evaluating a resultset by iterating over the rows explicitly is possible, of course.
//! Note that the row iterator returns <code>HdbResult&lt;Row&gt;</code>, not <code>Row</code>,
//! because the resultset might need to fetch more rows lazily from the server, which can fail.
//!
//! ```ignore
//! for row in resultset {
//!     let row = row?;
//!     ...
//! }
//! ```
//!
//! Such a streaming-like behavior is especially appropriate For large resultsets.
//! Iterating over the rows, while they are fetched on-demand from the server in smaller portions,
//! makes it easy to write complex evaluations in an efficient and scalable manner.
//!
//! ```ignore
//! let key_figure = resultset.into_iter()?.map(|r|{r?}).filter(...).fold(...);
//! ```
//!
//! You _can_ retrieve the field values of a row  individually, in arbitrary order.
//! <code>`hdbconnect::Row`</code> provides for this a single
//! method that is generalized by its return value,
//! so you need to specify the target type explicitly:
//!
//! ```ignore
//! for row in resultset {
//!     let row:Row = row?;
//!     let f4: NaiveDateTime = row.field_into(3)?;
//!     let f1: String = row.field_into(0)?;
//!     let f3: i32 = row.field_into(2)?;
//!     let f2: Option<i32> = row.field_into(1)?;
//! }
//! ```
//!
//! A usually more convenient way is to convert the complete row into a normal rust value
//! or tuple or struct:
//!
//! ```ignore
//! for row in connection.query("select * from TEST_RESULTSET")? {
//!     let td: TestData = row?.into_typed()?;
//! }
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
//!   ```ignore
//!   #[derive(Deserialize)]
//!   struct MyRow {
//!        ...
//!   }
//!
//!   let result: Vec<MyRow> = resultset.into_typed()?;
//!   ```
//!
//! * If the resultset contains only a single line (e.g. because you specified
//!   TOP 1 in your select, or you qualified the full primary key),
//!   then you can choose to deserialize into a plain <code>`MyRow`</code> directly.
//!
//!   ```ignore
//!    #[derive(Deserialize)]
//!    struct MyRow {
//!        ...
//!    }
//!
//!   let result: MyRow = resultset.into_typed()?;
//!   ```
//!
//! * If the resultset contains only a single column, then you can choose to
//!   deserialize into a <code>Vec&lt;field&gt;</code>,
//!   where <code>field</code> is a type that matches the field of the resultset.
//!   If a plain rust type is used, you don't even need to derive Deserialize:
//!
//!   ```ignore
//!   let result: Vec<u32> = resultset.into_typed()?;
//!   ```
//!
//! * If the resultset contains only a single value (one row with one column),
//!   then you can also deserialize into a plain <code>field</code>:
//!
//!   ```ignore
//!   let result: u32 = resultset.into_typed()?;
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
