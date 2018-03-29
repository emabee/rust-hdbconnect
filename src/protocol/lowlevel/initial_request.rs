use super::PrtResult;
use byteorder::{BigEndian, LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io;
use std::io::{BufRead, Write};
use std::net::TcpStream;

pub fn send_and_receive(stream: &mut TcpStream) -> PrtResult<(i8, i16)> {
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
    stream.write_i32::<BigEndian>(FILLER)?;
    stream.write_i8(MAJOR_PRODUCT_VERSION)?;
    stream.write_i16::<BigEndian>(MINOR_PRODUCT_VERSION)?;
    stream.write_i8(MAJOR_PROTOCOL_VERSION)?;
    stream.write_i16::<BigEndian>(MINOR_PROTOCOL_VERSION)?;
    stream.write_i8(RESERVED)?;

    stream.write_i8(NUMBER_OF_OPTIONS)?;
    stream.write_i8(OPTION_ID_SWAPKIND)?;
    stream.write_i8(LITTLE_ENDIAN)?;
    stream.flush()?;

    let mut rdr = io::BufReader::new(stream);
    let major_product_version: i8 = rdr.read_i8()?;
    let minor_product_version: i16 = rdr.read_i16::<LittleEndian>()?;
    rdr.consume(5); // ignore the rest (04:01:00:00:00)?
    debug!("successfully initialized");
    Ok((major_product_version, minor_product_version))
}
