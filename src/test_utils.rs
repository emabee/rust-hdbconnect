use Connection;
use DbcResult;


pub fn statement_ignore_err(connection: &mut Connection, stmts: Vec<&str>) {
    for s in stmts {
        match connection.any_statement(s) {
            Ok(_) => {}
            Err(_) => {}
        }
    }
}

pub fn multiple_statements(connection: &mut Connection, prep: Vec<&str>) -> DbcResult<()> {
    for s in prep {
        try!(connection.any_statement(s));
    }
    Ok(())
}
