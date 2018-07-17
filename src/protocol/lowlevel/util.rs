use byteorder::WriteBytesExt;
use std::io;
use std::iter::repeat;
use HdbResult;

/// Read n bytes from a `BufRead`, return as Vec<u8>
pub fn parse_bytes(len: usize, rdr: &mut io::BufRead) -> HdbResult<Vec<u8>> {
    let mut vec: Vec<u8> = repeat(255u8).take(len).collect();
    let mut read = 0;
    while read < len {
        read += rdr.read(&mut vec[read..])?;
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
