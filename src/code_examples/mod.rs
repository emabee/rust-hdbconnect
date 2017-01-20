//! Here are some code examples for the usage of this database driver.
//!
//! <b>1. Get an authenticated database connection</b>
//!
//!  Establish a physical connecton to the database server ...
//!
//!  ```ignore
//!  let (host, port): (&str, &str) = ...;
//!  let mut connection = Connection::new(host,port)?;
//!  ```
//!
//!  .. and authenticate to the database:
//!
//!  ```ignore
//!  let (user, pw): (&str, &str) = ...;
//!  connection.authenticate_user_password(user, pw)?;
//!  ```
//!
//!
//! <b>2. Query the database</b>
//!
//!  The most generic way to fire SQL statements without preparation is using
//!  <code>Connection::any_statement()</code>.
//!  This generic method can handle all kinds of calls,
//!  and thus has the most complex return type, <code>HdbResponse</code>.
//!
//!  ```ignore
//!  let my_statement = "..."; // some statement that doesn't need preparation
//!  let response: HdbResponse = connection.any_statement(my_statement)?;
//!  ```
//!
//!  <code>HdbResponse</code> is quite a complex nested enum which covers all possible
//!  return values we can get from the database.
//!  You thus have to analyze it to come to the
//!  concrete response relevant for your call.
//!  You can do this either explicitly using <code>match</code> etc or with the
//!  adequate short-cut method, e.g.:
//!
//!  ```ignore
//!  let resultset: ResultSet = response.as_resultset()?;
//!  ```
//!
//!  You can do the same of course with <code>HdbResponse</code>s obtained from the execution
//!  of prepared statements.
//!
//!  In many cases it will be more appropriate and convenient to use one of the
//!  specialized methods
//!
//!  * <code>query_statement()</code> -> ResultSet
//!  * <code>dml_statement()</code> -> usize
//!  * <code>exec_statement()</code> -> ()
//!
//!  which have the adequate simple result type you usually want:
//!
//!  ```ignore
//!  let my_statement = "..."; // some statement that doesn't need preparation
//!  let resultset: ResultSet = connection.query_statement(my_statement)?;
//!  ```
//!
//!  * In many cases, you will need or want to use prepared statements.
//!  Then the code will look like this:
//!
//!  ```ignore
//!  let stmt_str = "insert into TEST_PREPARE (F_STRING, F_INTEGER) values(?, ?)";
//!  let mut stmt = connection.prepare(stmt_str)?;
//!  stmt.add_batch(&("foo", 45_i32))?;
//!  stmt.add_batch(&("bar", 46_i32))?;
//!  stmt.execute_batch()?;
//!  ```
//!
//!  Or like this:
//!
//!  ```ignore
//!  let stmt_str = "select NAME, CITY from TEST_TABLE where age > ?";
//!  let mut stmt = connection.prepare(stmt_str)?;
//!  stmt.add_batch(&(45_i32))?;
//!  let resultset: ResultSet = stmt.execute_batch()?;
//!  ```
//!
//!  * In many cases, you will need or want to use prepared statements.
//!  Then the code will look like this:
//!
//!  ```ignore
//!  let stmt_str = "insert into TEST_PREPARE (F_STRING, F_INTEGER) values(?, ?)";
//!  let mut stmt = try!(connection.prepare(stmt_str));
//!  try!(stmt.add_batch(&("foo", 45_i32)));
//!  try!(stmt.add_batch(&("bar", 46_i32)));
//!  try!(stmt.execute_batch());
//!  ```
//!
//!  Or like this:
//!
//!  ```ignore
//!  let stmt_str = "select NAME, CITY from TEST_TABLE where age > ?";
//!  let mut stmt = try!(connection.prepare(stmt_str));
//!  try!(stmt.add_batch(&(45_i32)));
//!  let resultset: ResultSet = try!(stmt.execute_batch());
//!  ```
//!
//!
//! <b>3. Evaluate a resultset</b>
//!
//!  Evaluating a resultset by traversing rows and columns should always be possible,
//!  of course, but there's again a more convenient alternative.
//!  Thanks to the usage of serde you can convert the resultset directly
//!  into a fitting rust structure.
//!
//!  Note that you need to specify the type of your target variable explicitly, so that
//!  <code>ResultSet::into_typed(self)</code> can derive the type it needs to serialize into.
//!
//!  Depending on the usecase, <code>ResultSet::into_typed(self)</code>
//!  supports a variety of target data structures,
//!  with the only strong limitation that no data loss is supported.
//!
//!  * It depends on the <b>dimension of the resultset</b> what target data structures
//!    you can choose for deserialization:
//!
//!      * You can always use a <code>Vec&lt;MyRow&gt;</code>, where
//!        <code>MyRow</code> is a struct or tuple that matches the fields of the resultset.
//!
//!        ```ignore
//!         #[derive(Deserialize)]
//!         struct MyRow {
//!             ...
//!         }
//!
//!        let result: Vec<MyRow> = resultset.into_typed()?;
//!        ```
//!
//!      * If the resultset contains only a single line (e.g. because you specified
//!        TOP 1 in your select, or qualified the full primary key),
//!        then you can optionally choose to deserialize into a plain <code>MyRow</code> directly.
//!
//!        ```ignore
//!         #[derive(Deserialize)]
//!         struct MyRow {
//!             ...
//!         }
//!
//!        let result: MyRow = resultset.into_typed()?;
//!        ```
//!
//!      * If the resultset contains only a single column, then you can optionally choose to
//!        deserialize into a <code>Vec&lt;field&gt;</code>,
//!        where <code>field</code> is a type that matches the field of the resultset.
//!
//!        ```ignore
//!        let result: Vec<u32> = resultset.into_typed()?;
//!        ```
//!
//!      * If the resultset contains only a single value (one row with one column),
//!        then you can optionally choose to deserialize into a plain <code>MyRow</code>,
//!        or a <code>Vec&lt;field&gt;</code>, or a <code>field</code>.
//!
//!        ```ignore
//!        let result: u32 = resultset.into_typed()?;
//!        ```
//!
//!  * Also the <b>(de)serialization of the individual field values</b> provides flexibility.
//!      * You can e.g. convert values from a nullable column into a plain field,
//!        provided that no NULL values are given in the resultset.
//!
//!      * Vice versa, you always can use an Option<code>&lt;field&gt;</code>,
//!        even if the column is marked as NOT NULL.
//!
//!      * Similarly, integer types can differ, as long as the returned values can
//!        be assigned without loss.
//!
