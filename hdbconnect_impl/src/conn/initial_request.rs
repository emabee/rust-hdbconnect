#[cfg(feature = "async")]
use crate::protocol::util_async;

#[cfg(feature = "sync")]
use crate::protocol::util_sync;

use crate::{HdbError, HdbResult, conn::TcpClient};
use byteorder::{BigEndian, WriteBytesExt};
use std::{io::Write, sync::OnceLock};

#[cfg(feature = "sync")]
pub(crate) fn send_and_receive_sync(sync_tcp_connection: &mut TcpClient) -> HdbResult<()> {
    trace!("send_and_receive_sync(): send");
    match sync_tcp_connection {
        TcpClient::SyncPlain(pc) => {
            emit_initial_request_sync(pc.writer()).map_err(|e| HdbError::Initialization {
                source: Box::new(e),
            })?;
        }
        TcpClient::SyncTls(tc) => {
            emit_initial_request_sync(tc.writer()).map_err(|e| HdbError::TlsInit {
                source: Box::new(e),
            })?;
        }
        TcpClient::Dead { .. } => unreachable!(),
        #[cfg(feature = "async")]
        _ => unreachable!("Async connections not supported here"),
    }

    trace!("send_and_receive(): receive");
    // ignore the response content
    match sync_tcp_connection {
        TcpClient::SyncPlain(pc) => {
            util_sync::skip_bytes(8, pc.reader()).map_err(|e| HdbError::Initialization {
                source: Box::new(e),
            })
        }
        TcpClient::SyncTls(tc) => {
            util_sync::skip_bytes(8, tc.reader()).map_err(|e| HdbError::TlsInit {
                source: Box::new(e),
            })
        }
        TcpClient::Dead { .. } => unreachable!(),
        #[cfg(feature = "async")]
        _ => unreachable!("Async connections not supported here"),
    }
    .map_err(|e| {
        trace!("Skipping over empty initial response failed with {e:?}");
        e
    })?;
    debug!("Successfully initialized");
    Ok(())
}

#[cfg(feature = "async")]
pub(crate) async fn send_and_receive_async(tcp_client: &mut TcpClient) -> HdbResult<()> {
    trace!("send_and_receive_async(): send");
    match tcp_client {
        TcpClient::AsyncPlain(pa) => {
            emit_initial_request_async(pa.writer()).await.map_err(|e| {
                HdbError::Initialization {
                    source: Box::new(e),
                }
            })?;
        }
        TcpClient::AsyncTls(ta) => {
            emit_initial_request_async(ta.writer())
                .await
                .map_err(|e| HdbError::TlsInit {
                    source: Box::new(e),
                })?;
        }
        TcpClient::Dead { .. } => unreachable!(),
        #[cfg(feature = "sync")]
        _ => unreachable!("Sync connections not supported here"),
    }

    trace!("send_and_receive_async(): receive");
    // ignore the response content
    match tcp_client {
        TcpClient::AsyncPlain(tc) => {
            util_async::skip_bytes(8, tc.reader())
                .await
                .map_err(|e| HdbError::Initialization {
                    source: Box::new(e),
                })
        }
        TcpClient::AsyncTls(ta) => {
            util_async::skip_bytes(8, ta.reader())
                .await
                .map_err(|e| HdbError::TlsInit {
                    source: Box::new(e),
                })
        }
        TcpClient::Dead { .. } => unreachable!(),
        #[cfg(feature = "sync")]
        _ => unreachable!("Sync connections not supported here"),
    }?;

    debug!("Successfully initialized");
    Ok(())
}

#[cfg(feature = "sync")]
fn emit_initial_request_sync(w: &mut dyn std::io::Write) -> std::io::Result<()> {
    w.write_all(initial_request())?;
    w.flush()
}

#[cfg(feature = "async")]
async fn emit_initial_request_async<W: std::marker::Unpin + tokio::io::AsyncWriteExt>(
    w: &mut W,
) -> std::io::Result<()> {
    w.write_all(initial_request()).await?;
    w.flush().await
}

fn initial_request() -> &'static [u8] {
    static INITIAL_REQUEST: OnceLock<Box<[u8]>> = OnceLock::new();
    INITIAL_REQUEST.get_or_init(|| {
        const FILLER: i32 = -1;
        const MAJOR_PRODUCT_VERSION: i8 = 4;
        const MINOR_PRODUCT_VERSION: i16 = 20;
        const MAJOR_PROTOCOL_VERSION: i8 = 4;
        const MINOR_PROTOCOL_VERSION: i16 = 1;
        const RESERVED: i8 = 0;
        const NUMBER_OF_OPTIONS: i8 = 1;
        const OPTION_ID_SWAPKIND: i8 = 1;
        const LITTLE_ENDIAN: i8 = 1;

        let mut w = std::io::Cursor::new(vec![0_u8; 14]);
        Write::write_all(&mut w, &FILLER.to_be_bytes()).unwrap();
        WriteBytesExt::write_i8(&mut w, MAJOR_PRODUCT_VERSION).unwrap();
        WriteBytesExt::write_i16::<BigEndian>(&mut w, MINOR_PRODUCT_VERSION).unwrap();
        WriteBytesExt::write_i8(&mut w, MAJOR_PROTOCOL_VERSION).unwrap();
        WriteBytesExt::write_i16::<BigEndian>(&mut w, MINOR_PROTOCOL_VERSION).unwrap();
        WriteBytesExt::write_i8(&mut w, RESERVED).unwrap();
        WriteBytesExt::write_i8(&mut w, NUMBER_OF_OPTIONS).unwrap();
        WriteBytesExt::write_i8(&mut w, OPTION_ID_SWAPKIND).unwrap();
        WriteBytesExt::write_i8(&mut w, LITTLE_ENDIAN).unwrap();
        Write::flush(&mut w).unwrap();
        let res = w.into_inner().into_boxed_slice();
        assert_eq!(res.len(), 14);
        res
    })
}
