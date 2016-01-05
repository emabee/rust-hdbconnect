struct Transaction {
    ...
}
impl Transaction {
    /// Execute a statement and expect either a ResultSet or a RowsAffected
    pub fn call(&self, stmt: String, parameters: &[&ToHdb]) -> DbcResult<CallableStatementResult> {
        panic!("FIXME");
    }

    /// Execute a statement and expect a ResultSet
    pub fn query(&self, stmt: String, parameters: &[&ToHdb]) -> DbcResult<ResultSet> {
        panic!("FIXME");
    }

    /// Execute a statement and expect a RowsAffected
    pub fn execute(&self, stmt: String, parameters: &[&ToHdb]) -> DbcResult<RowsAffected> {
        panic!("FIXME");
    }

    /// Prepare a statement
    pub fn prepare(&self, stmt: String) -> DbcResult<PreparedStatement> {
        panic!("FIXME");
    }

    pub fn commit(&self, stmt: String) -> DbcResult<()> {
        panic!("FIXME");
    }

    pub fn rollback(&self, stmt: String) -> DbcResult<()> {
        panic!("FIXME");
    }
}
