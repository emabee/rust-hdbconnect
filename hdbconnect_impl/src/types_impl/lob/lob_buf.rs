// Vec is filled with push, and start denotes where "real" data starts.
// When drained until only a cesu8 tail is left, a reset is necessary
// (where the tail is moved to the Vec start) and a refill adds more data to the end.
#[derive(Clone)]
pub(crate) struct LobBuf {
    data: Vec<u8>,
    start: usize,
}

impl LobBuf {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        LobBuf {
            data: Vec::with_capacity(capacity),
            start: 0,
        }
    }
    pub(crate) fn with_initial_content(data: Vec<u8>) -> Self {
        Self { data, start: 0 }
    }

    pub(crate) fn into_inner(mut self) -> Vec<u8> {
        self.data.drain(0..self.start);
        self.data
    }

    #[allow(dead_code)]
    pub(crate) fn capacity(&self) -> usize {
        self.data.capacity()
    }

    fn start(&self) -> usize {
        self.start
    }

    fn set_start(&mut self, new_start: usize) {
        self.start = new_start;
    }

    fn end(&self) -> usize {
        self.data.len()
    }

    pub(crate) fn len(&self) -> usize {
        self.data.len() - self.start
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.data.len() == self.start
    }

    pub(crate) fn append(&mut self, data: &[u8]) {
        self.data.extend_from_slice(data);
    }

    pub(crate) fn drain(&mut self, n: usize) -> Result<&[u8], std::io::Error> {
        let start = self.start();
        let end = self.end();
        if n > end - start {
            return Err(std::io::Error::other("not enough data"));
        }
        self.set_start(start + n);
        Ok(&self.data[start..start + n])
    }
    #[allow(dead_code)]
    fn reset(&mut self) {
        let start = self.start();
        self.data.drain(0..start);
        self.set_start(0);
    }
}

impl std::ops::Index<usize> for LobBuf {
    type Output = u8;
    fn index(&self, index: usize) -> &Self::Output {
        &self.data[self.start + index]
    }
}

#[cfg(test)]
mod test {
    use super::LobBuf;

    #[test]
    fn test_lob_buf() {
        const CAPA: usize = 1000;
        let mut lob_buf = LobBuf::with_capacity(CAPA);
        assert_eq!(lob_buf.start(), 0);
        assert_eq!(lob_buf.end(), 0);
        assert_eq!(lob_buf.capacity(), CAPA);

        //                     123456789012345678901234567890123456789012345678901234567890
        lob_buf.append(b"laewirua4wltrkdfjgdfkxjghlaoeurhgndyflfkepwaoiregthdrfjgbfd,g");
        assert_eq!(lob_buf.start(), 0);
        assert_eq!(lob_buf.end(), 61);
        assert_eq!(lob_buf.capacity(), CAPA);

        let data = lob_buf.drain(43).unwrap();
        assert_eq!(data, b"laewirua4wltrkdfjgdfkxjghlaoeurhgndyflfkepw");
        assert_eq!(lob_buf.start(), 43);
        assert_eq!(lob_buf.end(), 61);

        let data = lob_buf.drain(15).unwrap();
        assert_eq!(data, b"aoiregthdrfjgbf");
        assert_eq!(lob_buf.start(), 58);
        assert_eq!(lob_buf.end(), 61);

        lob_buf.reset();
        assert_eq!(lob_buf.start(), 0);
        assert_eq!(lob_buf.end(), 3);
        let data = lob_buf.drain(3).unwrap();
        assert_eq!(data, b"d,g");
    }
}
