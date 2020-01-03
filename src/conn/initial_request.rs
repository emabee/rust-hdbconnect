use crate::conn::TcpConn;
use crate::protocol::util;

use byteorder::{BigEndian, WriteBytesExt};

pub(crate) fn send_and_receive(tcp_conn: &mut TcpConn) -> std::io::Result<()> {
    trace!("send_and_receive()");
    match tcp_conn {
        TcpConn::SyncPlain(ref pc) => {
            let writer = &mut *(pc.writer()).borrow_mut();
            emit_initial_request(writer)?;
        }
        TcpConn::SyncSecure(ref sc) => {
            let writer = &mut *(sc.writer()).borrow_mut();
            emit_initial_request(writer)?;
        }
    }

    match tcp_conn {
        TcpConn::SyncPlain(ref pc) => {
            let reader = &mut *(pc.reader()).borrow_mut();
            util::skip_bytes(8, reader)?; // ignore the response content
        }
        TcpConn::SyncSecure(ref sc) => {
            let reader = &mut *(sc.reader()).borrow_mut();
            util::skip_bytes(8, reader)?; // ignore the response content
        }
    }

    debug!("successfully initialized");
    Ok(())
}

fn emit_initial_request<W: std::io::Write>(w: &mut W) -> std::io::Result<()> {
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
