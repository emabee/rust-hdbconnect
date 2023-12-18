use std::io::Read;

pub(crate) struct LoggingReader<'a> {
    buf: Vec<u8>,
    given_out: u64,
    inner: &'a mut dyn Read,
}
impl<'a> LoggingReader<'a> {
    pub(crate) fn new(w: &'a mut dyn Read) -> LoggingReader {
        LoggingReader {
            buf: Vec::new(),
            given_out: 0,
            inner: w,
        }
    }
}
impl<'a> Read for LoggingReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self.inner.read(buf) {
            Ok(n) => {
                self.buf
                    .append(&mut buf[0..n].iter().cloned().collect::<Vec<u8>>());
                self.given_out += n as u64;
                Ok(n)
            }
            Err(e) => Err(e),
        }
    }
}

impl<'a> Drop for LoggingReader<'a> {
    fn drop(&mut self) {
        trace!(
            "FROM THE WIRE: {} bytes |{}|",
            self.buf.len(),
            self.buf
                .iter()
                // .take(10)
                .map(|b| format!("{b:02x} "))
                .collect::<String>()
        );
        trace!("Given out: {}", self.given_out);
    }
}
