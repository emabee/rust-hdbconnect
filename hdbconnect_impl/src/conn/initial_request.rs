#[cfg(feature = "async")]
use crate::protocol::util_async;

#[cfg(feature = "sync")]
use crate::protocol::util_sync;

use crate::{conn::TcpClient, HdbResult};
use byteorder::{BigEndian, WriteBytesExt};
use std::{io::Write, sync::OnceLock};

#[cfg(feature = "sync")]
pub(crate) fn send_and_receive_sync(sync_tcp_connection: &mut TcpClient) -> HdbResult<()> {
    trace!("send_and_receive(): send");
    match sync_tcp_connection {
        TcpClient::SyncPlain(ref mut pc) => {
            emit_initial_request_sync(pc.writer())?;
        }
        TcpClient::SyncTls(ref mut tc) => {
            emit_initial_request_sync(tc.writer())?;
        }
        #[cfg(feature = "async")]
        _ => unreachable!("Async connections not supported here"),
    }

    trace!("send_and_receive(): receive");
    match sync_tcp_connection {
        TcpClient::SyncPlain(ref mut pc) => {
            util_sync::skip_bytes(8, pc.reader()) // ignore the response content
        }
        TcpClient::SyncTls(ref mut tc) => {
            util_sync::skip_bytes(8, tc.reader()) // ignore the response content
        }
        #[cfg(feature = "async")]
        _ => unreachable!("Async connections not supported here"),
    }
    .map_err(|e| {
        trace!("Skipping over empty initial response failed with {:?}", e);
        e
    })?;
    debug!("Successfully initialized");
    Ok(())
}

#[cfg(feature = "async")]
pub(crate) async fn send_and_receive_async(tcp_client: &mut TcpClient) -> HdbResult<()> {
    trace!("send_and_receive(): send");
    match tcp_client {
        TcpClient::AsyncPlain(ref mut pa) => emit_initial_request_async(pa.writer()).await,
        TcpClient::AsyncTls(ref mut ta) => emit_initial_request_async(ta.writer()).await,
        TcpClient::Dead => unreachable!(),
        #[cfg(feature = "sync")]
        _ => unreachable!("Sync connections not supported here"),
    }?;

    trace!("send_and_receive(): receive");
    // ignore the response content
    match tcp_client {
        TcpClient::AsyncPlain(tc) => util_async::skip_bytes(8, tc.reader()).await,
        TcpClient::AsyncTls(ta) => util_async::skip_bytes(8, ta.reader()).await,
        TcpClient::Dead => unreachable!(),
        #[cfg(feature = "sync")]
        _ => unreachable!("Sync connections not supported here"),
    }?;

    debug!("Successfully initialized");
    Ok(())
}

#[cfg(feature = "sync")]
fn emit_initial_request_sync(w: &mut dyn std::io::Write) -> HdbResult<()> {
    w.write_all(initial_request())?;
    Ok(w.flush()?)
}

#[cfg(feature = "async")]
async fn emit_initial_request_async<W: std::marker::Unpin + tokio::io::AsyncWriteExt>(
    w: &mut W,
) -> HdbResult<()> {
    w.write_all(initial_request()).await?;
    Ok(w.flush().await?)
}

fn initial_request() -> &'static [u8] {
    static INITIAL_REQUEST: OnceLock<Box<[u8]>> = OnceLock::new();
    let result = INITIAL_REQUEST.get_or_init(|| {
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
    });
    result
}
