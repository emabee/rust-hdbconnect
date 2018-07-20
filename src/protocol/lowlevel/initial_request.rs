use protocol::lowlevel::util;
use HdbResult;

use byteorder::{BigEndian, LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io;
use std::net::TcpStream;

pub fn send_and_receive(stream: &mut TcpStream) -> HdbResult<(i8, i16)> {
    const FILLER: i32 = -1;
    const MAJOR_PRODUCT_VERSION: i8 = 4;
    const MINOR_PRODUCT_VERSION: i16 = 20;
    const MAJOR_PROTOCOL_VERSION: i8 = 4;
    const MINOR_PROTOCOL_VERSION: i16 = 1;
    const RESERVED: i8 = 0;
    const NUMBER_OF_OPTIONS: i8 = 1;
    const OPTION_ID_SWAPKIND: i8 = 1;
    const LITTLE_ENDIAN: i8 = 1;

    trace!("send_and_receive()");
    {
        let w = &mut io::BufWriter::with_capacity(20, &*stream);
        w.write_i32::<BigEndian>(FILLER)?;
        w.write_i8(MAJOR_PRODUCT_VERSION)?;
        w.write_i16::<BigEndian>(MINOR_PRODUCT_VERSION)?;
        w.write_i8(MAJOR_PROTOCOL_VERSION)?;
        w.write_i16::<BigEndian>(MINOR_PROTOCOL_VERSION)?;
        w.write_i8(RESERVED)?;

        w.write_i8(NUMBER_OF_OPTIONS)?;
        w.write_i8(OPTION_ID_SWAPKIND)?;
        w.write_i8(LITTLE_ENDIAN)?;
    }

    {
        let mut rdr = io::BufReader::new(stream);
        let major_product_version = rdr.read_i8()?;
        let minor_product_version = rdr.read_i16::<LittleEndian>()?;
        util::skip_bytes(5, &mut rdr)?; // ignore the rest (04:01:00:00:00)?
        debug!("successfully initialized");
        Ok((major_product_version, minor_product_version))
    }
}
