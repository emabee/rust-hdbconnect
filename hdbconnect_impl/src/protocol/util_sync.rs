use crate::HdbResult;

// Read n bytes, return as Vec<u8>
pub(crate) fn parse_bytes(len: usize, rdr: &mut dyn std::io::Read) -> HdbResult<Vec<u8>> {
    let mut buf: Vec<u8> = vec![0; len];
    rdr.read_exact(&mut buf)?;
    Ok(buf)
}

pub(crate) fn skip_bytes(n: usize, rdr: &mut dyn std::io::Read) -> HdbResult<()> {
    const MAXBUFLEN: usize = 16;
    if n > MAXBUFLEN {
        Err(crate::HdbError::Impl("n > MAXBUFLEN (16)"))
    } else {
        let mut buffer = [0_u8; MAXBUFLEN];
        Ok(rdr.read_exact(&mut buffer[0..n])?)
    }
}
