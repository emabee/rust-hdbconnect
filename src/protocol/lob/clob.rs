use protocol::cesu8;
use protocol::conn_core::AmConnCore;
use protocol::lob::fetch_a_lob_chunk::fetch_a_lob_chunk;
use protocol::server_resource_consumption_info::ServerResourceConsumptionInfo;
use std::cell::RefCell;
use std::cmp::max;
use std::io::{self, Write};
use std::sync::Arc;
use {HdbError, HdbResult};

/// CLOB implementation that is used with `TypedValue::CLOB` and
/// `TypedValue::NCLOB`.
#[derive(Clone, Debug)]
pub struct CLOB(RefCell<ClobHandle>);

pub fn new_clob_from_db(
    am_conn_core: &AmConnCore,
    is_data_complete: bool,
    length_c: u64,
    length_b: u64,
    locator_id: u64,
    data: &[u8],
) -> CLOB {
    CLOB(RefCell::new(ClobHandle::new(
        am_conn_core,
        is_data_complete,
        length_c,
        length_b,
        locator_id,
        data,
    )))
}

impl CLOB {
    /// Length of contained String
    pub fn len(&self) -> HdbResult<usize> {
        self.0.borrow_mut().len()
    }

    /// Is container empty
    pub fn is_empty(&self) -> HdbResult<bool> {
        Ok(self.len()? == 0)
    }

    /// Returns the maximum size of the internal buffers.
    ///
    /// Tests can verify that this value does not exceed `lob_read_size` +
    /// `buf.len()`.
    pub fn max_size(&self) -> usize {
        self.0.borrow().max_size()
    }

    /// Returns the contained String.
    pub fn into_string(self) -> HdbResult<String> {
        trace!("CLOB::into_string()");
        self.0.into_inner().into_string()
    }
}

// Support for CLOB streaming
impl io::Read for CLOB {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.borrow_mut().read(buf)
    }
}

/// `ClobHandle` is used for CLOBs and NCLOBs that we receive from the database.
/// The data are often not transferred completely, so we carry internally
/// a database connection and the
/// necessary controls to support fetching remaining data on demand.
#[derive(Clone, Debug)]
struct ClobHandle {
    o_am_conn_core: Option<AmConnCore>,
    is_data_complete: bool,
    length_c: u64,
    length_b: u64,
    locator_id: u64,
    buffer_cesu8: Vec<u8>,
    utf8: Vec<u8>,
    max_size: usize,
    acc_byte_length: usize,
    server_resource_consumption_info: ServerResourceConsumptionInfo,
}
impl ClobHandle {
    pub fn new(
        am_conn_core: &AmConnCore,
        is_data_complete: bool,
        length_c: u64,
        length_b: u64,
        locator_id: u64,
        cesu8: &[u8],
    ) -> ClobHandle {
        let acc_byte_length = cesu8.len();
        let mut utf8 = Vec::<u8>::new();
        let (success, _, byte_count) = cesu8::decode_from_iter(&mut utf8, &mut cesu8.iter());
        if !success && byte_count < cesu8.len() as u64 - 5 {
            error!("ClobHandle::new() bad cesu8 in first part of CLOB");
        }
        let (_u, c) = cesu8.split_at(byte_count as usize);
        let clob_handle = ClobHandle {
            o_am_conn_core: Some(Arc::clone(am_conn_core)),
            length_c,
            length_b,
            is_data_complete,
            locator_id,
            buffer_cesu8: Vec::<u8>::from(c),
            max_size: utf8.len() + c.len(),
            utf8,
            acc_byte_length,
            server_resource_consumption_info: Default::default(),
        };
        trace!(
            "ClobHandle::new() with: is_data_complete = {}, length_c = {}, length_b = {}, \
             locator_id = {}, buffer_cesu8.len() = {}, utf8.len() = {}",
            clob_handle.is_data_complete,
            clob_handle.length_c,
            clob_handle.length_b,
            clob_handle.locator_id,
            clob_handle.buffer_cesu8.len(),
            clob_handle.utf8.len()
        );
        clob_handle
    }

    pub fn len(&mut self) -> HdbResult<usize> {
        Ok(self.length_b as usize)
    }

    fn fetch_next_chunk(&mut self) -> HdbResult<()> {
        trace!(
            "ClobHandle.fetch_next_chunk(), utf8.len() = {}",
            self.utf8.len()
        );
        if self.is_data_complete {
            return Err(HdbError::impl_("fetch_next_chunk: clob already complete"));
        }
        let (mut reply_data, reply_is_last_data) = fetch_a_lob_chunk(
            &mut self.o_am_conn_core,
            self.locator_id,
            self.length_b,
            self.acc_byte_length as u64,
            &mut self.server_resource_consumption_info,
        )?;

        self.acc_byte_length += reply_data.len();
        self.buffer_cesu8.append(&mut reply_data);
        let (success, _, byte_count) =
            cesu8::decode_from_iter(&mut self.utf8, &mut self.buffer_cesu8.iter());

        if !success && byte_count < self.buffer_cesu8.len() as u64 - 5 {
            error!(
                "ClobHandle::fetch_next_chunk(): bad cesu8 at pos {} in part of CLOB: {:?}",
                byte_count, self.buffer_cesu8
            );
            return Err(HdbError::Cesu8(cesu8::Cesu8DecodingError));
        }

        // cut off the big first part (in most cases all) of buffer_cesu8, and retain
        // just the rest
        self.buffer_cesu8.drain(0..byte_count as usize);
        self.is_data_complete = reply_is_last_data;
        self.max_size = max(self.utf8.len() + self.buffer_cesu8.len(), self.max_size);

        assert_eq!(
            self.is_data_complete,
            self.length_b == self.acc_byte_length as u64
        );
        Ok(())
    }

    fn load_complete(&mut self) -> HdbResult<()> {
        trace!("ClobHandle::load_complete()");
        while !self.is_data_complete {
            self.fetch_next_chunk()?;
        }
        Ok(())
    }

    pub fn max_size(&self) -> usize {
        self.max_size
    }

    /// Converts a ClobHandle into a String containing its data.
    pub fn into_string(mut self) -> HdbResult<String> {
        trace!("ClobHandle::into_string()");
        self.load_complete()?;
        String::from_utf8(self.utf8).map_err(|_| HdbError::Cesu8(cesu8::Cesu8DecodingError))
    }
}

// Support for CLOB streaming
impl io::Read for ClobHandle {
    fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
        trace!("ClobHandle::read() with buf of len {}", buf.len());

        while !self.is_data_complete && (buf.len() > self.utf8.len()) {
            self.fetch_next_chunk()
                .map_err(|e| io::Error::new(io::ErrorKind::UnexpectedEof, e))?;
        }

        // we want to keep clean UTF-8 in utf8, so we cut off at good places only
        let count: usize = if self.utf8.len() < buf.len() {
            self.utf8.len()
        } else {
            let mut tmp = buf.len();
            while !cesu8::is_utf8_char_start(self.utf8[tmp]) {
                tmp -= 1;
            }
            tmp
        };

        buf.write_all(&self.utf8[0..count])?;
        self.utf8.drain(0..count);
        Ok(count)
    }
}
