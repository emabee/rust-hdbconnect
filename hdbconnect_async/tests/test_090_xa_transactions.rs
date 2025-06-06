extern crate serde;

mod test_utils;

#[cfg(feature = "dist_tx")]
mod a {
    use dist_tx::a_sync::tm::*;
    use flexi_logger::LoggerHandle;
    use hdbconnect_async::{Connection, HdbResult};
    use log::{debug, info};

    #[tokio::test] // cargo test --test test_090_xa_transactions -- --nocapture
    pub async fn test_090_xa_transactions() -> HdbResult<()> {
        let mut log_handle = super::test_utils::init_logger();
        let start = std::time::Instant::now();
        let connection = super::test_utils::get_authenticated_connection().await?;

        prepare(&mut log_handle, &connection).await?;

        successful_xa(&mut log_handle, &connection).await?;
        xa_rollback(&mut log_handle, &connection).await?;
        xa_repeated(&mut log_handle, &connection).await?;
        xa_conflicts(&mut log_handle, &connection).await?;

        super::test_utils::closing_info(connection, start).await
    }

    // prepare the db table
    async fn prepare(_log_handle: &mut LoggerHandle, conn: &Connection) -> HdbResult<()> {
        info!("Prepare...");
        conn.multiple_statements_ignore_err(vec!["drop table TEST_XA"])
            .await;
        conn.multiple_statements(vec![
            "create column table TEST_XA (f1 INT primary key, f2 NVARCHAR(20))",
            "insert into TEST_XA (f1, f2) values(-100, 'INITIAL')",
            "insert into TEST_XA (f1, f2) values(-101, 'INITIAL')",
            "insert into TEST_XA (f1, f2) values(-102, 'INITIAL')",
            "insert into TEST_XA (f1, f2) values(-103, 'INITIAL')",
        ])
        .await
    }

    async fn successful_xa(_log_handle: &mut LoggerHandle, conn: &Connection) -> HdbResult<()> {
        info!("Successful XA");

        // open two connections, auto_commit off
        let conn_a = conn.spawn().await?;
        conn_a.set_auto_commit(false).await;
        let conn_b = conn_a.spawn().await?;
        assert!(!conn_a.is_auto_commit().await);
        assert!(!conn_b.is_auto_commit().await);

        // instantiate a SimpleTransactionManager and register Resource Managers for
        // the two connections
        let mut tm = SimpleTransactionManager::new("test_090_xa_transactions");
        tm.register(conn_a.get_resource_manager(), 22, true)
            .await
            .unwrap();
        tm.register(conn_b.get_resource_manager(), 44, true)
            .await
            .unwrap();

        // start ta
        tm.start_transaction().await.unwrap();

        debug!("do some inserts");
        conn_a.dml(&insert_stmt(1, "a")).await?;
        conn_b.dml(&insert_stmt(2, "b")).await?;

        debug!("verify with neutral conn that nothing is visible (count)");
        let count_query = "select count(*) from TEST_XA where f1 > 0 and f1 < 9";
        let count: u32 = conn.query(count_query).await?.try_into().await?;
        assert_eq!(0, count);

        debug!("commit ta");
        tm.commit_transaction().await.unwrap();

        debug!("verify that stuff is now visible");
        let count: u32 = conn.query(count_query).await?.try_into().await?;
        assert_eq!(2, count);

        Ok(())
    }

    fn insert_stmt(i: u32, s: &'static str) -> String {
        format!("insert into TEST_XA (f1, f2) values({i}, '{s}')")
    }

    async fn xa_rollback(_log_handle: &mut LoggerHandle, conn: &Connection) -> HdbResult<()> {
        info!("xa_rollback");

        // open two connections, auto_commit off
        let conn_a = conn.spawn().await?;
        conn_a.set_auto_commit(false).await;
        let conn_b = conn_a.spawn().await?;
        assert!(!conn_a.is_auto_commit().await);
        assert!(!conn_b.is_auto_commit().await);
        let conn_c = conn.spawn().await?;

        conn_a
            .exec("SET TRANSACTION LOCK WAIT TIMEOUT 3000")
            .await
            .unwrap(); // (milliseconds)
        conn_b
            .exec("SET TRANSACTION LOCK WAIT TIMEOUT 3000")
            .await
            .unwrap(); // (milliseconds)
        conn_c
            .exec("SET TRANSACTION LOCK WAIT TIMEOUT 3000")
            .await
            .unwrap(); // (milliseconds)

        // instantiate a SimpleTransactionManager and register Resource Managers for
        // the two connections
        let mut tm = SimpleTransactionManager::new("test_090_xa_transactions");
        tm.register(conn_a.get_resource_manager(), 22, true)
            .await
            .unwrap();
        tm.register(conn_b.get_resource_manager(), 44, true)
            .await
            .unwrap();

        // start ta
        tm.start_transaction().await.unwrap();

        debug!("conn_a inserts");
        conn_a.dml(&insert_stmt(10, "a")).await?;
        conn_a.dml(&insert_stmt(11, "a")).await?;
        debug!("conn_b inserts");
        conn_b.dml(&insert_stmt(12, "b")).await?;
        conn_b.dml(&insert_stmt(13, "b")).await?;

        // verify with neutral conn that nothing is visible (count)
        let count_query = "select count(*) from TEST_XA where f1 > 9 and f1 < 99";
        let count: u32 = conn.query(count_query).await?.try_into().await?;
        assert_eq!(0, count);

        debug!("rollback xa");
        tm.rollback_transaction().await.unwrap();

        // verify that nothing additional was inserted
        let count: u32 = conn.query(count_query).await?.try_into().await?;
        assert_eq!(0, count);

        debug!("conn_c inserts");
        conn_c.dml(&insert_stmt(10, "c")).await?;
        conn_c.dml(&insert_stmt(11, "c")).await?;
        conn_c.dml(&insert_stmt(12, "c")).await?;
        conn_c.dml(&insert_stmt(13, "c")).await?;
        conn_c.commit().await.unwrap();

        // verify that now the insertions were successful
        let count: u32 = conn.query(count_query).await?.try_into().await?;
        assert_eq!(4, count);

        Ok(())
    }

    async fn xa_repeated(_log_handle: &mut LoggerHandle, conn: &Connection) -> HdbResult<()> {
        info!("xa_repeated");

        // open two connections, auto_commit off
        let conn_a = conn.spawn().await?;
        conn_a.set_auto_commit(false).await;
        let conn_b = conn_a.spawn().await?;
        assert!(!conn_a.is_auto_commit().await);
        assert!(!conn_b.is_auto_commit().await);

        conn_a
            .exec("SET TRANSACTION LOCK WAIT TIMEOUT 3000")
            .await
            .unwrap(); // (milliseconds)
        conn_b
            .exec("SET TRANSACTION LOCK WAIT TIMEOUT 3000")
            .await
            .unwrap(); // (milliseconds)

        // instantiate a SimpleTransactionManager and register Resource Managers for
        // the two connections
        let mut tm = SimpleTransactionManager::new("test_090_xa_transactions");
        tm.register(conn_a.get_resource_manager(), 22, true)
            .await
            .unwrap();
        tm.register(conn_b.get_resource_manager(), 44, true)
            .await
            .unwrap();

        for i in 0..5 {
            let j = i * 10 + 20;
            let count_query = format!(
                "select count(*) from TEST_XA where f1 > {} and f1 < {}",
                j,
                j + 9
            );

            tm.start_transaction().await.unwrap();

            debug!("conn_a inserts {j}");
            conn_a.dml(insert_stmt(j + 1, "a")).await?;
            conn_a.dml(insert_stmt(j + 2, "a")).await?;
            debug!("conn_b inserts {j}");
            conn_b.dml(insert_stmt(j + 3, "b")).await?;
            conn_b.dml(insert_stmt(j + 4, "b")).await?;

            // verify with neutral conn that nothing is visible (count)
            let count: u32 = conn.query(&count_query).await?.try_into().await?;
            assert_eq!(0, count);

            debug!("rollback xa");
            tm.rollback_transaction().await.unwrap();

            tm.start_transaction().await.unwrap();
            debug!("conn_a inserts {j}");
            conn_a.dml(insert_stmt(j + 1, "a")).await?;
            conn_a.dml(insert_stmt(j + 2, "a")).await?;
            debug!("conn_b inserts");
            conn_b.dml(insert_stmt(j + 3, "b")).await?;
            conn_b.dml(insert_stmt(j + 4, "b")).await?;

            // verify with neutral conn that nothing is visible (count)
            let count: u32 = conn.query(&count_query).await?.try_into().await?;
            assert_eq!(0, count);

            debug!("commit xa");
            tm.commit_transaction().await.unwrap();

            // verify that now the insertions were successful
            let count: u32 = conn.query(&count_query).await?.try_into().await?;
            assert_eq!(4, count);
        }

        Ok(())
    }

    async fn xa_conflicts(_log_handle: &mut LoggerHandle, conn: &Connection) -> HdbResult<()> {
        info!("xa_conflicts");

        // open two connections, auto_commit off
        let conn_a = conn.spawn().await?;
        conn_a.set_auto_commit(false).await;
        let conn_b = conn_a.spawn().await?;
        assert!(!conn_a.is_auto_commit().await);
        assert!(!conn_b.is_auto_commit().await);

        conn_a
            .exec("SET TRANSACTION LOCK WAIT TIMEOUT 3000")
            .await?;
        conn_b
            .exec("SET TRANSACTION LOCK WAIT TIMEOUT 3000")
            .await?;

        // instantiate a SimpleTransactionManager and register Resource Managers for
        // the two connections
        let mut tm = SimpleTransactionManager::new("test_090_xa_transactions");
        tm.register(conn_a.get_resource_manager(), 22, true)
            .await
            .unwrap();
        tm.register(conn_b.get_resource_manager(), 44, true)
            .await
            .unwrap();

        // do conflicting inserts
        // catch error response
        // try to commit
        // clean up
        // do clean inserts (to ensure nothing is left over)

        // do conflicting inserts
        // catch error response
        // rollback
        // do clean inserts (to ensure nothing is left over)

        Ok(())
    }
}
