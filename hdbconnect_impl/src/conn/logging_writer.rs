use std::io::Write;

pub(crate) struct LoggingWriter<'a> {
    buf: Vec<u8>,
    inner: &'a mut dyn Write,
}
impl<'a> LoggingWriter<'a> {
    pub(crate) fn new(w: &'a mut dyn Write) -> LoggingWriter {
        LoggingWriter {
            buf: Vec::new(),
            inner: w,
        }
    }
}
impl<'a> Write for LoggingWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buf.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        trace!(
            "TO THE WIRE: {} bytes |{}|",
            self.buf.len(),
            self.buf
                .iter()
                .take(10)
                .map(|b| format!("{b:02x} "))
                .collect::<String>()
        );

        self.inner.write_all(&self.buf)?;
        self.inner.flush()?;
        self.buf.clear();
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use byteorder::{LittleEndian, WriteBytesExt};
    use std::io::Write;

    #[test]
    fn test_logging_writer() {
        let mut dumbuf = Vec::<u8>::new();
        let mut lw = super::LoggingWriter::new(&mut dumbuf);
        lw.write_u32::<LittleEndian>(23_u32).unwrap();
        write!(lw, "{}", "Hello world").unwrap();
        lw.flush().unwrap();

        lw.write_u32::<LittleEndian>(98_u32).unwrap();
        write!(lw, "{}", "fantastic").unwrap();
        lw.flush().unwrap();
    }
}
