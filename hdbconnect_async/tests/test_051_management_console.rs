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
        match hdb_return_value {
            HdbReturnValue::ResultSet(result_set) => {
                println!("{:?}", result_set);
            }
            HdbReturnValue::AffectedRows(vec_usize) => {
                for val in vec_usize {
                    println!("Affected rows: {}", val);
                }
            }
            HdbReturnValue::OutputParameters(output_parameters) => {
                println!("Output parameters");
                for op in output_parameters.into_values().into_iter() {
                    println!("   Output parameter: {:?}", op);
                    // FIXME all three methods should be async and functionally complete
                    match op {
                        HdbValue::BLOB(blob) => {
                            println!("Value: {:?}", blob.into_bytes().await?);
                        }
                        HdbValue::CLOB(clob) => {
                            println!("Value: {}", clob.into_string()?);
                        }
                        HdbValue::NCLOB(nclob) => {
                            println!("Value: {}", nclob.into_string()?);
                        }
                        _ => {
                            println!("Value: {}", op);
                        }
                    }
                }
            }
            HdbReturnValue::Success => {
                println!("Success");
            }
            HdbReturnValue::XaTransactionIds(vec_ta_ids) => {
                println!("Transaction-ids");
                for val in vec_ta_ids {
                    println!("   transaction-id: {:?}", val);
                }
            }
        }
    }
    Ok(())
}
