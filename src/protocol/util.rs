use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io;
use std::iter::repeat;
use HdbResult;

/// Read n bytes from a `BufRead`, return as Vec<u8>
pub fn parse_bytes(len: usize, rdr: &mut io::BufRead) -> HdbResult<Vec<u8>> {
    let mut vec: Vec<u8> = repeat(255u8).take(len).collect();
    {
        let rf: &mut [u8] = &mut vec;
        rdr.read_exact(rf)?;
    }
    Ok(vec)
}

/// Write a byte vec to a Write impl
pub fn serialize_bytes(v: &[u8], w: &mut io::Write) -> HdbResult<()> {
    for b in v {
        w.write_u8(*b)?;
    }
    Ok(())
}

pub fn skip_bytes(n: usize, rdr: &mut io::BufRead) -> HdbResult<()> {
    trace!("Skipping over {} padding bytes", n);
    for _ in 0..n {
        rdr.read_u8()?;
    }
    Ok(())
}

// FIXME dont_use_soft_consume_bytes
pub fn dont_use_soft_consume_bytes(n: usize, rdr: &mut io::BufRead) -> HdbResult<()> {
    trace!("Maybe skipping over {} padding bytes", n);
    rdr.consume(n);
    Ok(())
}
