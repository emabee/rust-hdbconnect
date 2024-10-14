// import java.sql.*;
// public class jdemo {
//    public static void main(String[] argv) {
//       Connection connection = null;
//       try {
//          connection = DriverManager.getConnection(
//             "jdbc:sap://myhdb:30715/?autocommit=false", "myName",
//                "mySecret");
//       } catch (SQLException e) {
//          System.err.println("Connection Failed:");
//          System.err.println(e);
//          return;
//       }
//       if (connection != null) {
//          try {
//             System.out.println("Connection to HANA successful!");
//             Statement stmt = connection.createStatement();
//             ResultSet resultSet = stmt.executeQuery("Select 'Hello', 'world' from dummy");
//             resultSet.next();
//             String hello = resultSet.getString(1);
//             String world = resultSet.getString(2);
//             System.out.println(hello + " " + world);
//        } catch (SQLException e) {
//           System.err.println("Query failed!");
//        }
//      }
//    }
// }

use hdbconnect::{ConnectParamsBuilder, Connection, ConnectionConfiguration, HdbResult};

pub fn main() -> HdbResult<()> {
    let connection = Connection::with_configuration(
        ConnectParamsBuilder::from("hdbsql://myhdb:30715")?
            .with_dbuser("myName")
            .with_password("mySecret"),
        &ConnectionConfiguration::default().with_auto_commit(false),
    )?;
    println!("Connection to HANA successful!");

    let (h, w): (String, String) = connection
        .query("select 'Hello', 'world' from dummy")?
        .try_into()?;
    println!("{h} {w}!");
    Ok(())
}
// readability AND safety:
// no boilerplate for error handling, API safety, direct assignment of result set
