use crate::{HdbError, HdbResult};

// Read n bytes, return as Vec<u8>
// pub(crate) async fn parse_bytes<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
//     len: usize,
//     rdr: &mut R,
// ) -> HdbResult<Vec<u8>> {
//     let mut vec: Vec<u8> = std::iter::repeat(255_u8).take(len).collect();
//     {
//         let rf: &mut [u8] = &mut vec;
//         rdr.read_exact(rf).await?;
//     }
//     Ok(vec)
// }

pub(crate) async fn skip_bytes<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    n: usize,
    rdr: &mut R,
) -> HdbResult<()> {
    const MAXBUFLEN: usize = 16;
    if n > MAXBUFLEN {
        Err(HdbError::Impl("impl: n > MAXBUFLEN (16)"))
    } else {
        let mut buffer = [0_u8; MAXBUFLEN];
        let _tmp: usize = rdr.read_exact(&mut buffer[0..n]).await?;
        Ok(())
    }
}
