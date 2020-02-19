use crate::conn::TcpClient;
use crate::protocol::util;

use byteorder::{BigEndian, WriteBytesExt};

pub(crate) fn send_and_receive(tcp_conn: &mut TcpClient) -> std::io::Result<()> {
    trace!("send_and_receive()");
    trace!("send_and_receive(): send");
    match tcp_conn {
        TcpClient::SyncPlain(ref pc) => {
            emit_initial_request(&mut *(pc.writer()).borrow_mut())?;
        }
        TcpClient::SyncTls(ref tc) => {
            emit_initial_request(&mut *(tc.writer()).borrow_mut())?;
        }
        #[cfg(feature = "alpha_nonblocking")]
        TcpClient::SyncNonblockingTls(ref mut nbtc) => {
            emit_initial_request(nbtc)?;
        }
    }

    trace!("send_and_receive(): receive");
    match tcp_conn {
        TcpClient::SyncPlain(ref pc) => {
            util::skip_bytes(8, &mut *(pc.reader()).borrow_mut()) // ignore the response content
        }
        TcpClient::SyncTls(ref tc) => {
            util::skip_bytes(8, &mut *(tc.reader()).borrow_mut()) // ignore the response content
        }
        #[cfg(feature = "alpha_nonblocking")]
        TcpClient::SyncNonblockingTls(ref mut nbtc) => {
            util::skip_bytes(8, nbtc) // ignore the response content
        }
    }
    .map_err(|e| {
        trace!("Skipping over empty initial response failed with {:?}", e);
        e
    })?;
    debug!("successfully initialized");
    Ok(())
}

fn emit_initial_request(w: &mut dyn std::io::Write) -> std::io::Result<()> {
    const FILLER: i32 = -1;
    const MAJOR_PRODUCT_VERSION: i8 = 4;
    const MINOR_PRODUCT_VERSION: i16 = 20;
    const MAJOR_PROTOCOL_VERSION: i8 = 4;
    const MINOR_PROTOCOL_VERSION: i16 = 1;
    const RESERVED: i8 = 0;
    const NUMBER_OF_OPTIONS: i8 = 1;
    const OPTION_ID_SWAPKIND: i8 = 1;
    const LITTLE_ENDIAN: i8 = 1;
    w.write_i32::<BigEndian>(FILLER)?;
    w.write_i8(MAJOR_PRODUCT_VERSION)?;
    w.write_i16::<BigEndian>(MINOR_PRODUCT_VERSION)?;
    w.write_i8(MAJOR_PROTOCOL_VERSION)?;
    w.write_i16::<BigEndian>(MINOR_PROTOCOL_VERSION)?;
    w.write_i8(RESERVED)?;

    w.write_i8(NUMBER_OF_OPTIONS)?;
    w.write_i8(OPTION_ID_SWAPKIND)?;
    w.write_i8(LITTLE_ENDIAN)?;
    w.flush()?;
    Ok(())
}
