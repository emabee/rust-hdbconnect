use super::TcpClient;
use crate::protocol::util;
use byteorder::{BigEndian, WriteBytesExt};
use std::io::Write;

pub(crate) fn send_and_receive(tcp_conn: &mut TcpClient) -> std::io::Result<()> {
    trace!("send_and_receive()");
    trace!("send_and_receive(): send");
    match tcp_conn {
        TcpClient::SyncPlain(ref mut pc) => {
            emit_initial_request(pc.writer())?;
        }
        TcpClient::SyncTls(ref mut tc) => {
            emit_initial_request(tc.writer())?;
        }
    }

    trace!("send_and_receive(): receive");
    match tcp_conn {
        TcpClient::SyncPlain(ref mut pc) => {
            util::skip_bytes(8, pc.reader()) // ignore the response content
        }
        TcpClient::SyncTls(ref mut tc) => {
            util::skip_bytes(8, tc.reader()) // ignore the response content
        }
    }
    .map_err(|e| {
        trace!("Skipping over empty initial response failed with {:?}", e);
        e
    })?;
    debug!("Successfully initialized");
    Ok(())
}

fn emit_initial_request(w: &mut dyn std::io::Write) -> std::io::Result<()> {
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
            c.write_all(&FILLER.to_be_bytes()).unwrap();
            c.write_i8(MAJOR_PRODUCT_VERSION).unwrap();
            c.write_i16::<BigEndian>(MINOR_PRODUCT_VERSION).unwrap();
            c.write_i8(MAJOR_PROTOCOL_VERSION).unwrap();
            c.write_i16::<BigEndian>(MINOR_PROTOCOL_VERSION).unwrap();
            c.write_i8(RESERVED).unwrap();
            c.write_i8(NUMBER_OF_OPTIONS).unwrap();
            c.write_i8(OPTION_ID_SWAPKIND).unwrap();
            c.write_i8(LITTLE_ENDIAN).unwrap();
            c.flush().unwrap();
            let res = c.into_inner().into_boxed_slice();
            assert_eq!(res.len(), 14);
            res
        };
    }
    w.write_all(&*INITIAL_REQUEST)?;
    w.flush()
}
