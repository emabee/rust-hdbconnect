extern crate serde;

mod test_utils;

// use flexi_logger::LoggerHandle;
use hdbconnect_async::{HdbResult, HdbReturnValue, HdbValue};
// use log::{debug, info};

#[tokio::test]
async fn test_051_management_console() -> HdbResult<()> {
    let connection = test_utils::get_authenticated_connection().await?;

    let mut stmt = connection
        .prepare("CALL MANAGEMENT_CONSOLE_PROC(?, ?, ?)")
        .await?;
    let hdb_response = stmt.execute(&("encryption status", "ld3670:30807")).await?;
    for hdb_return_value in hdb_response.into_iter() {
        #[allow(unreachable_patterns)] // needed to avoid wrong error message in VS Code
        match hdb_return_value {
            HdbReturnValue::ResultSet(result_set) => {
                println!("{result_set:?}");
            }
            HdbReturnValue::AffectedRows(vec_usize) => {
                for val in vec_usize {
                    println!("Affected rows: {val}");
                }
            }
            HdbReturnValue::OutputParameters(output_parameters) => {
                println!("Output parameters");
                for op in output_parameters.into_values().into_iter() {
                    println!("   Output parameter: {op:?}");
                    match op {
                        HdbValue::ASYNC_BLOB(blob) => {
                            println!("Value: {:?}", blob.into_bytes().await?);
                        }
                        HdbValue::ASYNC_CLOB(clob) => {
                            println!("Value: {}", clob.into_string().await?);
                        }
                        HdbValue::ASYNC_NCLOB(nclob) => {
                            println!("Value: {}", nclob.into_string().await?);
                        }
                        _ => {
                            println!("Value: {op}");
                        }
                    }
                }
            }
            HdbReturnValue::Success => {
                println!("Success");
            }
            #[cfg(feature = "dist_tx")]
            HdbReturnValue::XaTransactionIds(vec_ta_ids) => {
                println!("Transaction-ids");
                for val in vec_ta_ids {
                    println!("   transaction-id: {val:?}");
                }
            }
            _ => {}
        }
    }
    Ok(())
}
