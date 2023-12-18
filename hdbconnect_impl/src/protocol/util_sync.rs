use crate::HdbResult;

// Read n bytes, return as Vec<u8>
pub(crate) fn parse_bytes(len: usize, rdr: &mut dyn std::io::Read) -> HdbResult<Vec<u8>> {
    // FIXME: replace completely with version that takes the buffer as parameter
    let mut vec: Vec<u8> = std::iter::repeat(255_u8).take(len).collect();
    {
        let rf: &mut [u8] = &mut vec;
        rdr.read_exact(rf)?;
    }
    Ok(vec)
}

#[cfg(feature = "sync")] // is necessary
pub(crate) fn skip_bytes(n: usize, rdr: &mut dyn std::io::Read) -> HdbResult<()> {
    const MAXBUFLEN: usize = 16;
    if n > MAXBUFLEN {
        Err(crate::HdbError::Impl("n > MAXBUFLEN (16)"))
    } else {
        let mut buffer = [0_u8; MAXBUFLEN];
        Ok(rdr.read_exact(&mut buffer[0..n])?)
    }
}
