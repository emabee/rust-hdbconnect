#[cfg(feature = "async")]
use {
    crate::{conn::AsyncTcpClient, protocol::util_async},
    std::sync::Arc,
    tokio::{
        io::{AsyncWriteExt, BufWriter},
        net::TcpStream,
    },
};

#[cfg(feature = "sync")]
use crate::{conn::SyncTcpClient, protocol::util_sync};

use byteorder::{BigEndian, WriteBytesExt};
use std::io::Write;

#[cfg(feature = "sync")]
pub(crate) fn sync_send_and_receive(
    sync_tcp_connection: &mut SyncTcpClient,
) -> std::io::Result<()> {
    trace!("send_and_receive(): send");
    match sync_tcp_connection {
        SyncTcpClient::PlainSync(ref mut pc) => {
            sync_emit_initial_request(pc.writer())?;
        }
        SyncTcpClient::TlsSync(ref mut tc) => {
            sync_emit_initial_request(tc.writer())?;
        }
    }

    trace!("send_and_receive(): receive");
    match sync_tcp_connection {
        SyncTcpClient::PlainSync(ref mut pc) => {
            util_sync::skip_bytes(8, pc.reader()) // ignore the response content
        }
        SyncTcpClient::TlsSync(ref mut tc) => {
            util_sync::skip_bytes(8, tc.reader()) // ignore the response content
        }
    }
    .map_err(|e| {
        trace!("Skipping over empty initial response failed with {:?}", e);
        e
    })?;
    debug!("Successfully initialized");
    Ok(())
}

#[cfg(feature = "async")]
pub(crate) async fn async_send_and_receive(
    async_tcp_client: &mut AsyncTcpClient,
) -> std::io::Result<()> {
    trace!("send_and_receive(): send");
    match async_tcp_client {
        AsyncTcpClient::Plain(pc) => {
            async_emit_initial_request(pc.writer()).await?;
        }
    }

    trace!("send_and_receive(): receive");
    match async_tcp_client {
        AsyncTcpClient::Plain(tc) => {
            let am_rdr = tc.reader();
            let mut reader = am_rdr.lock().await;

            util_async::skip_bytes(8, &mut *reader).await?; // ignore the response content
        }
    }
    debug!("Successfully initialized");
    Ok(())
}

#[cfg(feature = "sync")]
fn sync_emit_initial_request(w: &mut dyn std::io::Write) -> std::io::Result<()> {
    w.write_all(initial_request())?;
    w.flush()
}

#[cfg(feature = "async")]
async fn async_emit_initial_request(
    am_w: Arc<tokio::sync::Mutex<BufWriter<TcpStream>>>,
) -> std::io::Result<()> {
    let mut writer = am_w.lock().await;
    let w = &mut *writer;
    w.write_all(initial_request()).await?;
    w.flush().await?;
    Ok(())
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
