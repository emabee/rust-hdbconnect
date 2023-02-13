use hdbconnect_async::HdbResult;

mod test_utils;

#[tokio::test]
async fn test_080_conn_pooling_for_rocket() -> HdbResult<()> {
    let _log_handle = test_utils::init_logger();
    if cfg!(feature = "rocket_pool") {
        log::info!("testing feature 'rocket_pool'");
        #[cfg(feature = "rocket_pool")]
        inner::test_rocket_pool().await?;
        Ok(())
    } else {
        log::info!("Nothing tested, because feature 'rocket_pool' is not active");
        Ok(())
    }
}

#[cfg(feature = "rocket_pool")]
mod inner {
    extern crate serde;

    use hdbconnect_async::{HanaPoolForRocket, HdbError, HdbResult};
    use log::trace;
    use rocket_db_pools::Pool;
    use tokio::task::JoinHandle;

    const NO_OF_WORKERS: usize = 20;
    pub(super) async fn test_rocket_pool() -> HdbResult<()> {
        let pool = HanaPoolForRocket::new(super::test_utils::get_std_cp_builder()?)?;

        let mut worker_handles: Vec<JoinHandle<u8>> = Default::default();

        for _ in 0..NO_OF_WORKERS {
            let pool_clone = pool.clone();
            worker_handles.push(tokio::spawn(async move {
                let conn = pool_clone.get().await.unwrap();
                trace!("connection[{}]: Firing query", conn.id().await.unwrap());
                conn.query("select 1 from dummy").await.unwrap();
                0_u8
            }));
        }

        for worker_handle in worker_handles {
            assert_eq!(
                0_u8,
                worker_handle
                    .await
                    .map_err(|e| HdbError::UsageDetailed(format!(
                        "Joining worker thread failed: {e:?}"
                    )))?
            );
        }

        Ok(())
    }
}
