use crate::{HdbResult, impl_err};

pub(crate) async fn skip_bytes<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    n: usize,
    rdr: &mut R,
) -> HdbResult<()> {
    const MAXBUFLEN: usize = 16;
    if n > MAXBUFLEN {
        Err(impl_err!("impl: n > MAXBUFLEN (16)"))
    } else {
        let mut buffer = [0_u8; MAXBUFLEN];
        let _tmp: usize = rdr.read_exact(&mut buffer[0..n]).await?;
        Ok(())
    }
}
