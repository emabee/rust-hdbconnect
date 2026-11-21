use hdbconnect_async::HdbResult;

mod test_utils;

#[tokio::test]
async fn test_081_conn_pooling_for_bb8() -> HdbResult<()> {
    let _log_handle = test_utils::init_logger();
    if cfg!(feature = "bb8_pool") {
        log::info!("testing feature 'bb8_pool'");
        #[cfg(feature = "bb8_pool")]
        {
            inner::test_bb8_pool().await?;
            // regression test for https://github.com/emabee/rust-hdbconnect/issues/65
            inner::test_checkout_does_not_stall().await?;
            // ensure a connection broken during use is not handed out again
            inner::test_broken_connection_is_discarded().await?;
        }
        Ok(())
    } else {
        log::info!("Nothing tested, because feature 'bb8_pool' is not active");
        Ok(())
    }
}

#[cfg(feature = "bb8_pool")]
mod inner {
    extern crate serde;
    use hdbconnect_async::{ConnectionConfiguration, ConnectionManager, HdbError, HdbResult};
    use log::trace;
    use std::{borrow::Cow, time::Duration, time::Instant};
    use tokio::task::JoinHandle;

    const NO_OF_WORKERS: usize = 20;
    pub(super) async fn test_bb8_pool() -> HdbResult<()> {
        let pool = bb8::Pool::builder()
            .max_size(15)
            .build(ConnectionManager::with_configuration(
                super::test_utils::get_std_cp_builder()?,
                ConnectionConfiguration::default().with_auto_commit(false),
            )?)
            .await?;

        let mut worker_handles: Vec<JoinHandle<u8>> = Default::default();

        for _ in 0..NO_OF_WORKERS {
            let pool_clone = pool.clone();
            worker_handles.push(tokio::spawn(async move {
                let conn = pool_clone.get().await.unwrap();
                trace!("connection[{}]: Firing query", conn.id().await);
                conn.query("select 1 from dummy").await.unwrap();
                0_u8
            }));
        }

        for worker_handle in worker_handles {
            assert_eq!(
                0_u8,
                worker_handle
                    .await
                    .map_err(|e| HdbError::Usage(Cow::from(format!(
                        "Joining worker thread failed: {e:?}"
                    ))))?
            );
        }

        Ok(())
    }

    // Regression test for https://github.com/emabee/rust-hdbconnect/issues/65.
    //
    // Checkout validation (`test_on_check_out`, which bb8 enables by default) used to stall for
    // up to the full `connection_timeout` (~30 s) whenever a checkout overlapped with the async
    // cleanup that runs when a `Connection` is dropped: dropping a connection spawns a task that
    // locks the connection mutex to send a disconnect, and the old `ValidityChecker` rebuilt its
    // `is_broken()` future on every `poll()`, losing its place in the mutex's waker queue. The
    // fix makes validation a plain `async fn`, so its wait for the mutex stays registered and
    // resumes as soon as cleanup releases the lock.
    //
    // We drive many sequential checkouts against a pool of size 1 with a deliberately short
    // connection_timeout. Each iteration drops the previous connection (triggering the cleanup
    // task) and immediately checks out again. Every individual checkout must complete far below
    // the timeout; before the fix, some of these checkouts stalled until the timeout fired.
    pub(super) async fn test_checkout_does_not_stall() -> HdbResult<()> {
        const CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);
        // A fast checkout is a single validation roundtrip; allow generous head-room for slow
        // links while staying clearly below CONNECTION_TIMEOUT so a real stall is caught.
        const MAX_ACCEPTABLE_CHECKOUT: Duration = Duration::from_secs(3);
        const ITERATIONS: usize = 30;

        let pool = bb8::Pool::builder()
            .max_size(1)
            .connection_timeout(CONNECTION_TIMEOUT)
            // leave test_on_check_out at its default (true) on purpose: that is the path that
            // triggered the stall.
            .build(ConnectionManager::with_configuration(
                super::test_utils::get_std_cp_builder()?,
                ConnectionConfiguration::default().with_auto_commit(false),
            )?)
            .await?;

        let mut worst = Duration::ZERO;
        for i in 0..ITERATIONS {
            let start = Instant::now();
            let conn = pool.get().await.map_err(|e| {
                HdbError::Usage(Cow::from(format!(
                    "checkout {i} failed or timed out after {:?}: {e:?}",
                    start.elapsed()
                )))
            })?;
            let elapsed = start.elapsed();
            worst = worst.max(elapsed);
            trace!("checkout {i} took {elapsed:?}");

            conn.query("select 1 from dummy").await?;
            // dropping `conn` here spawns the async cleanup that locks the connection mutex;
            // the next loop iteration checks out while that may still be in flight.
            drop(conn);

            assert!(
                elapsed < MAX_ACCEPTABLE_CHECKOUT,
                "checkout {i} took {elapsed:?}, which is close to the connection_timeout of \
                 {CONNECTION_TIMEOUT:?}; validation appears to stall (issue #65)"
            );
        }
        log::info!("test_checkout_does_not_stall: worst checkout was {worst:?}");

        Ok(())
    }

    // With `test_on_check_out(false)` there is no validation roundtrip on checkout, so a
    // connection that broke while in use must be reported as broken synchronously via
    // `ManageConnection::has_broken`; otherwise bb8 would keep handing the dead connection out.
    //
    // We break the connection the same way test_130_timeout does: a short read_timeout plus a
    // table lock held by a second connection makes the pooled query fail with
    // `ConnectionBroken`, which marks the connection dead. On return, bb8 calls `has_broken()`,
    // which now reads the shared flag set during the failed roundtrip and drops the connection.
    // The subsequent checkout must therefore yield a fresh, working connection.
    pub(super) async fn test_broken_connection_is_discarded() -> HdbResult<()> {
        const TIMEOUT: Duration = Duration::from_millis(150);
        const TABLE: &str = "TEST_081_BB8_BROKEN";

        // a separate, non-pooled connection that holds a row lock on the table
        let locker = super::test_utils::get_authenticated_connection_with_configuration(
            &ConnectionConfiguration::default().with_auto_commit(false),
        )
        .await?;
        locker
            .multiple_statements_ignore_err(vec![&format!("drop table {TABLE}")])
            .await;
        locker
            .multiple_statements(vec![&format!(
                "create table {TABLE} (F1_S NVARCHAR(20), F2_I INT)"
            )])
            .await?;
        locker.commit().await?;
        locker
            .dml(format!(
                "insert into {TABLE} (F1_S, F2_I) values('locker', 1)"
            ))
            .await?;
        // take and hold the lock (auto_commit is off, so the lock is kept)
        locker
            .query(format!("select * from {TABLE} for update"))
            .await?;

        let pool = bb8::Pool::builder()
            .max_size(1)
            .test_on_check_out(false)
            .connection_timeout(Duration::from_secs(5))
            .build(ConnectionManager::with_configuration(
                super::test_utils::get_std_cp_builder()?,
                ConnectionConfiguration::default()
                    .with_auto_commit(false)
                    .with_read_timeout(Some(TIMEOUT)),
            )?)
            .await?;

        // first checkout: run a query that blocks on the lock and times out, breaking the socket
        {
            let conn = pool
                .get()
                .await
                .map_err(|e| HdbError::Usage(Cow::from(format!("first checkout failed: {e:?}"))))?;
            let result = conn
                .query(format!("select * from {TABLE} for update"))
                .await;
            assert!(
                matches!(result, Err(HdbError::ConnectionBroken { .. })),
                "expected the blocked query to break the connection, got {result:?}"
            );
            assert!(
                conn.has_broken(),
                "connection broke during use but has_broken() still reports healthy (issue #65, \
                 point 1): a dead connection would be handed out again under test_on_check_out(false)"
            );
            // returning the broken connection to the pool; bb8 must discard it via has_broken()
        }

        // release the lock so the next connection can work
        locker.rollback().await?;

        // second checkout: must be a fresh, usable connection, not the dead one
        {
            let conn = pool.get().await.map_err(|e| {
                HdbError::Usage(Cow::from(format!("second checkout failed: {e:?}")))
            })?;
            assert!(
                !conn.has_broken(),
                "pool handed out a connection that is already marked broken"
            );
            conn.query("select 1 from dummy").await?;
        }

        locker
            .multiple_statements_ignore_err(vec![&format!("drop table {TABLE}")])
            .await;
        Ok(())
    }
}
