#[cfg(feature = "async")]
use crate::protocol::util_async;

#[cfg(feature = "sync")]
use crate::protocol::util_sync;

use crate::conn::TcpClient;
use byteorder::{BigEndian, WriteBytesExt};
use std::io::Write;

#[cfg(feature = "sync")]
pub(crate) fn sync_send_and_receive(sync_tcp_connection: &mut TcpClient) -> std::io::Result<()> {
    trace!("send_and_receive(): send");
    match sync_tcp_connection {
        TcpClient::SyncPlain(ref mut pc) => {
            sync_emit_initial_request(pc.writer())?;
        }
        TcpClient::SyncTls(ref mut tc) => {
            sync_emit_initial_request(tc.writer())?;
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
pub(crate) async fn async_send_and_receive(tcp_client: &mut TcpClient) -> std::io::Result<()> {
    trace!("send_and_receive(): send");
    match tcp_client {
        TcpClient::AsyncPlain(ref mut pa) => async_emit_initial_request(pa.writer()).await,
        TcpClient::AsyncTls(ref mut ta) => async_emit_initial_request(ta.writer()).await,
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
fn sync_emit_initial_request(w: &mut dyn std::io::Write) -> std::io::Result<()> {
    w.write_all(initial_request())?;
    w.flush()
}

#[cfg(feature = "async")]
async fn async_emit_initial_request<W: std::marker::Unpin + tokio::io::AsyncWriteExt>(
    w: &mut W,
) -> std::io::Result<()> {
    w.write_all(initial_request()).await?;
    w.flush().await
}

fn initial_request() -> &'static [u8] {
    lazy_static! {
        pub(crate) static ref INITIAL_REQUEST: Box<[u8]> = {
            const FILLER: i32 = -1;
            const MAJOR_PRODUCT_VERSION: i8 = 4;
            const MINOR_PRODUCT_VERSION: i16 = 20;
            const MAJOR_PROTOCOL_VERSION: i8 = 4;
            const MINOR_PROTOCOL_VERSION: i16 = 1;
            const RESERVED: i8 = 0;
            const NUMBER_OF_OPTIONS: i8 = 1;
            const OPTION_ID_SWAPKIND: i8 = 1;
            const LITTLE_ENDIAN: i8 = 1;

            let mut c = std::io::Cursor::new(vec![0_u8; 14]);
            Write::write_all(&mut c, &FILLER.to_be_bytes()).unwrap();
            WriteBytesExt::write_i8(&mut c, MAJOR_PRODUCT_VERSION).unwrap();
            WriteBytesExt::write_i16::<BigEndian>(&mut c, MINOR_PRODUCT_VERSION).unwrap();
            WriteBytesExt::write_i8(&mut c, MAJOR_PROTOCOL_VERSION).unwrap();
            WriteBytesExt::write_i16::<BigEndian>(&mut c, MINOR_PROTOCOL_VERSION).unwrap();
            WriteBytesExt::write_i8(&mut c, RESERVED).unwrap();
            WriteBytesExt::write_i8(&mut c, NUMBER_OF_OPTIONS).unwrap();
            WriteBytesExt::write_i8(&mut c, OPTION_ID_SWAPKIND).unwrap();
            WriteBytesExt::write_i8(&mut c, LITTLE_ENDIAN).unwrap();
            Write::flush(&mut c).unwrap();
            let res = c.into_inner().into_boxed_slice();
            assert_eq!(res.len(), 14);
            res
        };
    }
    &INITIAL_REQUEST
}
