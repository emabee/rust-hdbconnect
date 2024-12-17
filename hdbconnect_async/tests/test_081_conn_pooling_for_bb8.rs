use hdbconnect_async::HdbResult;

mod test_utils;

#[tokio::test]
async fn test_081_conn_pooling_for_bb8() -> HdbResult<()> {
    let _log_handle = test_utils::init_logger();
    if cfg!(feature = "bb8_pool") {
        log::info!("testing feature 'bb8_pool'");
        #[cfg(feature = "bb8_pool")]
        inner::test_bb8_pool().await?;
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
    use std::borrow::Cow;
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
}
