use conn_core::buffalo::Buffalo;
use protocol::util;
use HdbResult;

use byteorder::{BigEndian, WriteBytesExt};

pub fn send_and_receive(buffalo: &mut Buffalo) -> HdbResult<()> {
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
        let w = &mut *(buffalo.writer().borrow_mut());

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
    }

    {
        let r = &mut *(buffalo.reader().borrow_mut());
        util::skip_bytes(8, r)?; // ignore the response content
    }
    debug!("successfully initialized");
    Ok(())
}
