// Read n bytes, return as Vec<u8>
pub(crate) async fn parse_bytes<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    len: usize,
    rdr: &mut R,
) -> std::io::Result<Vec<u8>> {
    let mut vec: Vec<u8> = std::iter::repeat(255_u8).take(len).collect();
    {
        let rf: &mut [u8] = &mut vec;
        rdr.read_exact(rf).await?;
    }
    Ok(vec)
}

pub(crate) async fn skip_bytes<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    n: usize,
    rdr: &mut R,
) -> std::io::Result<()> {
    const MAXBUFLEN: usize = 16;
    if n > MAXBUFLEN {
        Err(crate::protocol::util::io_error("impl: n > MAXBUFLEN (16)"))
    } else {
        let mut buffer = [0_u8; MAXBUFLEN];
        let _tmp: usize = rdr.read_exact(&mut buffer[0..n]).await?;
        Ok(())
    }
}

pub(crate) async fn read_f64<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    rdr: &mut R,
) -> std::io::Result<f64> {
    let mut buffer = [0_u8; 8];
    rdr.read_exact(&mut buffer).await?;
    Ok(f64::from_le_bytes(buffer))
}

pub(crate) async fn read_i16<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    rdr: &mut R,
) -> std::io::Result<i16> {
    let mut buffer = [0_u8; 2];
    rdr.read_exact(&mut buffer).await?;
    Ok(i16::from_le_bytes(buffer))
}

pub(crate) async fn read_i32<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    rdr: &mut R,
) -> std::io::Result<i32> {
    let mut buffer = [0_u8; 4];
    rdr.read_exact(&mut buffer).await?;
    Ok(i32::from_le_bytes(buffer))
}

pub(crate) async fn read_i64<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    rdr: &mut R,
) -> std::io::Result<i64> {
    let mut buffer = [0_u8; 8];
    rdr.read_exact(&mut buffer).await?;
    Ok(i64::from_le_bytes(buffer))
}

pub(crate) async fn read_i128<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    rdr: &mut R,
) -> std::io::Result<i128> {
    let mut buffer = [0_u8; 16];
    rdr.read_exact(&mut buffer).await?;
    Ok(i128::from_le_bytes(buffer))
}

pub(crate) async fn read_u16<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    rdr: &mut R,
) -> std::io::Result<u16> {
    let mut buffer = [0_u8; 2];
    rdr.read_exact(&mut buffer).await?;
    Ok(u16::from_le_bytes(buffer))
}

pub(crate) async fn read_u32<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    rdr: &mut R,
) -> std::io::Result<u32> {
    let mut buffer = [0_u8; 4];
    rdr.read_exact(&mut buffer).await?;
    Ok(u32::from_le_bytes(buffer))
}

pub(crate) async fn read_u64<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    rdr: &mut R,
) -> std::io::Result<u64> {
    let mut buffer = [0_u8; 8];
    rdr.read_exact(&mut buffer).await?;
    Ok(u64::from_le_bytes(buffer))
}
