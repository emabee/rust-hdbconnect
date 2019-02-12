use super::{fetch_a_lob_chunk, CharLobSlice};
use crate::conn_core::AmConnCore;
use crate::protocol::server_resource_consumption_info::ServerResourceConsumptionInfo;
use crate::protocol::util;
use crate::{HdbError, HdbResult};
use serde_derive::Serialize;
use std::boxed::Box;
use std::cmp::max;
use std::io::{self, Write};

/// Character LOB implementation that is used with `HdbValue::CLOB`.
///
/// Note that the CLOB type is not recommended for use.
///
/// CLOB fields are supposed to only store ASCII7, but HANA doesn't check this.
/// So the implementation is a mixture of BLOB implementation (the protocol counts bytes, not chars)
/// and NCLOB implementation (the exposed data are chars, not bytes).
#[derive(Clone, Debug, Serialize)]
pub struct CLob(Box<CLobHandle>);

impl CLob {
    pub(crate) fn new(
        am_conn_core: &AmConnCore,
        is_data_complete: bool,
        total_char_length: u64,
        total_byte_length: u64,
        locator_id: u64,
        data: Vec<u8>,
    ) -> CLob {
        CLob(Box::new(CLobHandle::new(
            am_conn_core,
            is_data_complete,
            total_char_length,
            total_byte_length,
            locator_id,
            data,
        )))
    }

    /// Converts the CLob into the contained String.
    pub fn into_string(self) -> HdbResult<String> {
        trace!("CLob::into_string()");
        self.0.into_string()
    }

    /// Reads from given offset and the given length, in bytes.
    pub fn read_slice(&mut self, offset: u64, length: u32) -> HdbResult<CharLobSlice> {
        self.0.read_slice(offset, length)
    }

    /// Total length of data, in bytes.
    pub fn total_byte_length(&self) -> u64 {
        self.0.total_byte_length()
    }

    /// Returns true if the CLob does not contain data.
    pub fn is_empty(&self) -> bool {
        self.total_byte_length() == 0
    }

    /// Returns the maximum size of the internal buffer, in bytes.
    ///
    /// With streaming, this value should not exceed `lob_read_size` plus
    /// the buffer size used by the reader.
    pub fn max_buf_len(&self) -> usize {
        self.0.max_buf_len()
    }

    /// Current size of the internal buffer, in bytes.
    pub fn cur_buf_len(&self) -> usize {
        self.0.cur_buf_len() as usize
    }
}

// Support for CLob streaming
impl io::Read for CLob {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}

// `CLobHandle` is used for CLOBs that we receive from the database.
// The data are often not transferred completely, so we carry internally
// a database connection and the necessary controls to support fetching
// remaining data on demand.
// Since the data stream can be cut into chunks anywhere in the byte stream,
// we may need to buffer an orphaned part of a multi-byte sequence between two fetches.
#[derive(Clone, Debug, Serialize)]
struct CLobHandle {
    #[serde(skip)]
    o_am_conn_core: Option<AmConnCore>,
    is_data_complete: bool,
    total_char_length: u64,
    total_byte_length: u64,
    locator_id: u64,
    buffer_cesu8: Vec<u8>,
    utf8: String,
    max_buf_len: usize,
    acc_byte_length: usize,
    #[serde(skip)]
    server_resource_consumption_info: ServerResourceConsumptionInfo,
}
impl CLobHandle {
    fn new(
        am_conn_core: &AmConnCore,
        is_data_complete: bool,
        total_char_length: u64,
        total_byte_length: u64,
        locator_id: u64,
        cesu8: Vec<u8>,
    ) -> CLobHandle {
        let acc_byte_length = cesu8.len();

        let (utf8, buffer_cesu8) = util::to_string_and_tail(cesu8).unwrap(/* yes */);
        let clob_handle = CLobHandle {
            o_am_conn_core: Some(am_conn_core.clone()),
            total_char_length,
            total_byte_length,
            is_data_complete,
            locator_id,
            max_buf_len: utf8.len() + buffer_cesu8.len(),
            buffer_cesu8,
            utf8,
            acc_byte_length,
            server_resource_consumption_info: Default::default(),
        };
        debug!(
            "CLobHandle::new() with: is_data_complete = {}, total_char_length = {}, total_byte_length = {}, \
             locator_id = {}, buffer_cesu8.len() = {}, utf8.len() = {}",
            clob_handle.is_data_complete,
            clob_handle.total_char_length,
            clob_handle.total_byte_length,
            clob_handle.locator_id,
            clob_handle.buffer_cesu8.len(),
            clob_handle.utf8.len()
        );
        clob_handle
    }

    fn read_slice(&mut self, offset: u64, length: u32) -> HdbResult<CharLobSlice> {
        match self.o_am_conn_core {
            None => Err(HdbError::Usage(
                "Fetching more LOB chunks is no more possible (connection already closed)"
                    .to_owned(),
            )),
            Some(ref mut am_conn_core) => {
                let (reply_data, _reply_is_last_data) = fetch_a_lob_chunk(
                    am_conn_core,
                    self.locator_id,
                    offset,
                    length,
                    &mut self.server_resource_consumption_info,
                )?;

                debug!("read_slice(): got {} bytes", reply_data.len());

                Ok(util::split_off_orphaned_bytes(reply_data)?)
            }
        }
    }

    fn total_byte_length(&self) -> u64 {
        self.total_byte_length
    }

    fn cur_buf_len(&self) -> usize {
        self.utf8.len()
    }

    fn fetch_next_chunk(&mut self) -> HdbResult<()> {
        if self.is_data_complete {
            return Err(HdbError::impl_("fetch_next_chunk(): already complete"));
        }

        match self.o_am_conn_core {
            None => Err(HdbError::Usage(
                "Fetching more CLob chunks is no more possible (connection already closed)"
                    .to_owned(),
            )),
            Some(ref mut am_conn_core) => {
                let (mut reply_data, reply_is_last_data) = fetch_a_lob_chunk(
                    am_conn_core,
                    self.locator_id,
                    self.acc_byte_length as u64,
                    {
                        let guard = am_conn_core.lock()?;
                        std::cmp::min(
                            (*guard).get_lob_read_length() as u32,
                            (self.total_byte_length - self.acc_byte_length as u64) as u32,
                        )
                    },
                    &mut self.server_resource_consumption_info,
                )?;

                self.acc_byte_length += reply_data.len();
                if self.buffer_cesu8.is_empty() {
                    let (utf8, buffer) = util::to_string_and_tail(reply_data)?;
                    self.utf8.push_str(&utf8);
                    self.buffer_cesu8 = buffer;
                } else {
                    self.buffer_cesu8.append(&mut reply_data);
                    let mut buffer_cesu8 = vec![];
                    std::mem::swap(&mut buffer_cesu8, &mut self.buffer_cesu8);
                    let (utf8, buffer) = util::to_string_and_tail(buffer_cesu8)?;

                    self.utf8.push_str(&utf8);
                    self.buffer_cesu8 = buffer;
                }

                self.is_data_complete = reply_is_last_data;
                self.max_buf_len = max(self.utf8.len() + self.buffer_cesu8.len(), self.max_buf_len);

                if self.is_data_complete {
                    if self.total_byte_length != self.acc_byte_length as u64 {
                        warn!(
                            "is_data_complete: {}, total_byte_length: {}, acc_byte_length: {}",
                            self.is_data_complete, self.total_byte_length, self.acc_byte_length,
                        );
                    }
                    assert_eq!(self.total_byte_length, self.acc_byte_length as u64);
                } else {
                    assert!(self.total_byte_length != self.acc_byte_length as u64);
                }
                Ok(())
            }
        }
    }

    fn load_complete(&mut self) -> HdbResult<()> {
        trace!("load_complete()");
        while !self.is_data_complete {
            self.fetch_next_chunk()?;
        }
        Ok(())
    }

    fn max_buf_len(&self) -> usize {
        self.max_buf_len
    }

    // Converts a CLobHandle into a String containing its data.
    fn into_string(mut self) -> HdbResult<String> {
        trace!("into_string()");
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
