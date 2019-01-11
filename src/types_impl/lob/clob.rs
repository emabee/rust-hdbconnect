use super::fetch_a_lob_chunk;
use crate::conn_core::AmConnCore;
use crate::protocol::server_resource_consumption_info::ServerResourceConsumptionInfo;
use crate::protocol::util;
use crate::{HdbError, HdbResult};
use serde_derive::Serialize;
use std::cell::RefCell;
use std::cmp::max;
use std::io::{self, Write};

/// CLob implementation that is used with `HdbValue::CLOB`.
///
/// CLOB fields are supposed to only store ASCII7, but HANA doesn't check this.
/// So the implementation is a mixture of BLOB implementation (the protocol counts bytes, not chars)
/// and NCLOB implementation (the exposed data are chars, not bytes).
#[derive(Clone, Debug, Serialize)]
pub struct CLob(RefCell<CLobHandle>);

pub(crate) fn new_clob_from_db(
    am_conn_core: &AmConnCore,
    is_data_complete: bool,
    length_c: u64,
    length_b: u64,
    locator_id: u64,
    data: &[u8],
) -> CLob {
    CLob(RefCell::new(CLobHandle::new(
        am_conn_core,
        is_data_complete,
        length_c,
        length_b,
        locator_id,
        data,
    )))
}

impl CLob {
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
        trace!("CLob::into_string()");
        self.0.into_inner().into_string()
    }
}

// Support for CLob streaming
impl io::Read for CLob {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.borrow_mut().read(buf)
    }
}

// `CLobHandle` is used for CLOBs and NCLOBs that we receive from the database.
// The data are often not transferred completely, so we carry internally
// a database connection and the
// necessary controls to support fetching remaining data on demand.
#[derive(Clone, Debug, Serialize)]
struct CLobHandle {
    #[serde(skip)]
    o_am_conn_core: Option<AmConnCore>,
    is_data_complete: bool,
    length_c: u64,
    length_b: u64,
    locator_id: u64,
    buffer_cesu8: Vec<u8>,
    utf8: String,
    max_size: usize,
    acc_byte_length: usize,
    #[serde(skip)]
    server_resource_consumption_info: ServerResourceConsumptionInfo,
}
impl CLobHandle {
    pub fn new(
        am_conn_core: &AmConnCore,
        is_data_complete: bool,
        length_c: u64,
        length_b: u64,
        locator_id: u64,
        cesu8: &[u8],
    ) -> CLobHandle {
        let acc_byte_length = cesu8.len();

        let (utf8, buffer_cesu8) = util::to_string_and_tail(cesu8).unwrap(/* yes */);
        let clob_handle = CLobHandle {
            o_am_conn_core: Some(am_conn_core.clone()),
            length_c,
            length_b,
            is_data_complete,
            locator_id,
            max_size: utf8.len() + buffer_cesu8.len(),
            buffer_cesu8,
            utf8,
            acc_byte_length,
            server_resource_consumption_info: Default::default(),
        };
        debug!(
            "CLobHandle::new() with: is_data_complete = {}, length_c = {}, length_b = {}, \
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
        debug!("fetch_next_chunk(): utf8.len() = {}", self.utf8.len());
        if self.is_data_complete {
            return Err(HdbError::impl_("fetch_next_chunk(): already complete"));
        }
        let (mut reply_data, reply_is_last_data) = fetch_a_lob_chunk(
            &mut self.o_am_conn_core,
            self.locator_id,
            self.length_b,
            self.acc_byte_length as u64,
            &mut self.server_resource_consumption_info,
        )?;

        debug!("reply_data.len() = {}", reply_data.len());

        self.acc_byte_length += reply_data.len();
        self.buffer_cesu8.append(&mut reply_data);
        let (utf8, buffer) = util::to_string_and_tail(&self.buffer_cesu8)?;

        self.utf8.push_str(&utf8);
        self.buffer_cesu8 = buffer;
        self.is_data_complete = reply_is_last_data;
        self.max_size = max(self.utf8.len() + self.buffer_cesu8.len(), self.max_size);

        if self.is_data_complete {
            if self.length_b != self.acc_byte_length as u64 {
                warn!(
                    "is_data_complete: {}, length_b: {}, acc_byte_length: {}",
                    self.is_data_complete, self.length_b, self.acc_byte_length,
                );
            }
            assert_eq!(self.length_b, self.acc_byte_length as u64);
        } else {
            assert!(self.length_b != self.acc_byte_length as u64);
        }
        Ok(())
    }

    fn load_complete(&mut self) -> HdbResult<()> {
        trace!("load_complete()");
        while !self.is_data_complete {
            self.fetch_next_chunk()?;
        }
        Ok(())
    }

    pub fn max_size(&self) -> usize {
        self.max_size
    }

    /// Converts a CLobHandle into a String containing its data.
    pub fn into_string(mut self) -> HdbResult<String> {
        trace!("CLobHandle::into_string()");
        self.load_complete()?;
        Ok(self.utf8)
    }
}

// Support for CLOB streaming
impl io::Read for CLobHandle {
    fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
        trace!("CLobHandle::read() with buf of len {}", buf.len());

        while !self.is_data_complete && (buf.len() > self.utf8.len()) {
            self.fetch_next_chunk()
                .map_err(|e| io::Error::new(io::ErrorKind::UnexpectedEof, e))?;
        }

        // we want to keep clean UTF-8 in utf8, so we cut off at good places only
        let count: usize = if self.utf8.len() < buf.len() {
            self.utf8.len()
        } else {
            let mut tmp = buf.len();
            while !util::is_utf8_char_start(self.utf8.as_bytes()[tmp]) {
                tmp -= 1;
            }
            tmp
        };

        buf.write_all(&self.utf8.as_bytes()[0..count])?;
        self.utf8.drain(0..count);
        Ok(count)
    }
}
