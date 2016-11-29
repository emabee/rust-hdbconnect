//! Here are some code examples for the usage of this database driver.
//!
//! 1. Get an authenticated database connection.
//!
//!  Establish a physical connecton to the database server ...
//!
//!  ```ignore
//!  let (host, port): (&str, &str) = ...;
//!  let mut connection = try!(Connection::new(host,port));
//!  ```
//!
//!  .. and authenticate to the database:
//!
//!  ```ignore
//!  let (user, pw): (&str, &str) = ...;
//!  try!(connection.authenticate_user_password(user, pw));
//!  ```
//!
//!
//! 2. Query to the database.
//!
//!  Thanks to the usage of serde you can get the database result directly
//!  into a fitting rust structure.
//!  No need to traverse a resultset by row and column...
//!
//!
//!  ```ignore
//!  let stmt = "select \
//!                  LNAME as \"last_name\", \
//!                  FNAME as \"first_name\", \
//!                  MI as \"middle\", \
//!                  birthdate as \"birthdate\" \
//!              from COMPANY.EMPLOYEE \
//!              where id = 4711";
//!
//!  #[derive(Deserialize)]
//!  struct Person {
//!      last_name: String,
//!      first_name: String,
//!      middle: Option<char>,
//!      birthdate: Option<LongDate>,
//!  }
//!
//!  let result: Vec<Person> = try!(connection.query_statement(stmt));
//!  ```
//!
//!
//!
