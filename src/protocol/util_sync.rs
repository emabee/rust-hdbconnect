// Read n bytes, return as Vec<u8>
pub(crate) fn parse_bytes(len: usize, rdr: &mut dyn std::io::Read) -> std::io::Result<Vec<u8>> {
    let mut vec: Vec<u8> = std::iter::repeat(255_u8).take(len).collect();
    {
        let rf: &mut [u8] = &mut vec;
        rdr.read_exact(rf)?;
    }
    Ok(vec)
}

pub(crate) fn skip_bytes(n: usize, rdr: &mut dyn std::io::Read) -> std::io::Result<()> {
    const MAXBUFLEN: usize = 16;
    if n > MAXBUFLEN {
        Err(crate::protocol::util::io_error("impl: n > MAXBUFLEN (16)"))
    } else {
        let mut buffer = [0_u8; MAXBUFLEN];
        rdr.read_exact(&mut buffer[0..n])
    }
}
