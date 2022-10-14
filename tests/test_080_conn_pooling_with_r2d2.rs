extern crate serde;

mod test_utils;

use hdbconnect::{ConnectionManager, HdbResult};
use log::trace;
use std::thread::{self, JoinHandle};

#[test]
fn test_080_conn_pooling_with_r2d2() -> HdbResult<()> {
    //let mut log_handle = test_utils::init_logger();

    let manager = ConnectionManager::new(test_utils::get_std_cp_builder()?)?;
    let pool = r2d2::Pool::builder().max_size(15).build(manager).unwrap();

    let no_of_workers: usize = 20;
    let mut worker_handles: Vec<JoinHandle<u8>> = Default::default();

    for thread_number in 0..no_of_workers {
        let pool = pool.clone();
        worker_handles.push(
            thread::Builder::new()
                .name(thread_number.to_string())
                .spawn(move || {
                    let mut conn = pool.get().unwrap();
                    trace!("connection[{}]: Firing query", conn.id().unwrap());
                    conn.query("select 1 from dummy").unwrap();
                    0 as u8
                })
                .unwrap(),
        );
    }

    for worker_handle in worker_handles {
        worker_handle
            .join()
            .unwrap_or_else(|e| panic!("Joining worker thread failed: {:?}", e));
    }

    Ok(())
}
