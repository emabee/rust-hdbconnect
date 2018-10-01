use conn_core::AmConnCore;
use protocol::server_resource_consumption_info::ServerResourceConsumptionInfo;
use protocol::util;
use std::cell::RefCell;
use std::cmp::max;
use std::io::{self, Write};
use std::sync::Arc;
use types_impl::lob::fetch_a_lob_chunk;
use {HdbError, HdbResult};

/// NCLob implementation that is used with `HdbValue::NCLOB`.
#[derive(Clone, Debug)]
pub struct NCLob(RefCell<NCLobHandle>);

pub fn new_nclob_from_db(
    am_conn_core: &AmConnCore,
    is_data_complete: bool,
    length_c: u64,
    length_b: u64,
    locator_id: u64,
    data: &[u8],
) -> NCLob {
    NCLob(RefCell::new(NCLobHandle::new(
        am_conn_core,
        is_data_complete,
        length_c,
        length_b,
        locator_id,
        data,
    )))
}

impl NCLob {
    /// Length of contained String
    pub fn len(&self) -> HdbResult<usize> {
        self.0.borrow_mut().len()
    }

    /// Is container empty
    pub fn is_empty(&self) -> HdbResult<bool> {
        Ok(self.len()? == 0)
    }

    /// Returns the maximum size of the internal buffers.
    pub fn max_size(&self) -> usize {
        self.0.borrow().max_size()
    }

    /// Returns the contained String.
    pub fn into_string(self) -> HdbResult<String> {
        trace!("NCLob::into_string()");
        self.0.into_inner().into_string()
    }
}

// Support for NCLob streaming
impl io::Read for NCLob {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.borrow_mut().read(buf)
    }
}

// `NCLobHandle` is used for CLOBs and NCLOBs that we receive from the database.
// The data are often not transferred completely, so we carry internally
// a database connection and the
// necessary controls to support fetching remaining data on demand.
#[derive(Clone, Debug)]
struct NCLobHandle {
    o_am_conn_core: Option<AmConnCore>,
    is_data_complete: bool,
    length_c: u64,
    length_b: u64,
    locator_id: u64,
    surrogate_buf: Option<[u8; 3]>,
    utf8: String,
    max_size: usize,
    acc_byte_length: usize,
    acc_char_length: usize,
    server_resource_consumption_info: ServerResourceConsumptionInfo,
}
impl NCLobHandle {
    pub fn new(
        am_conn_core: &AmConnCore,
        is_data_complete: bool,
        length_c: u64,
        length_b: u64,
        locator_id: u64,
        cesu8: &[u8],
    ) -> NCLobHandle {
        let acc_byte_length = cesu8.len();
        let acc_char_length = util::count_1_2_3_sequence_starts(cesu8);

        let (utf8, surrogate_buf) = util::to_string_and_surrogate(cesu8).unwrap(/* yes */);

        let nclob_handle = NCLobHandle {
            o_am_conn_core: Some(Arc::clone(am_conn_core)),
            length_c,
            length_b,
            is_data_complete,
            locator_id,
            max_size: utf8.len() + if surrogate_buf.is_some() { 3 } else { 0 },
            surrogate_buf,
            utf8,
            acc_byte_length,
            acc_char_length,
            server_resource_consumption_info: Default::default(),
        };

        debug!(
            "new() with: is_data_complete = {}, length_c = {}, length_b = {}, \
             locator_id = {}, surrogate_buf = {:?}, utf8.len() = {}",
            nclob_handle.is_data_complete,
            nclob_handle.length_c,
            nclob_handle.length_b,
            nclob_handle.locator_id,
            nclob_handle.surrogate_buf,
            nclob_handle.utf8.len()
        );
        nclob_handle
    }

    pub fn len(&mut self) -> HdbResult<usize> {
        Ok(self.length_b as usize)
    }

    fn fetch_next_chunk(&mut self) -> HdbResult<()> {
        if self.is_data_complete {
            return Err(HdbError::impl_("fetch_next_chunk(): already complete"));
        }

        let (mut reply_data, reply_is_last_data) = fetch_a_lob_chunk(
            &mut self.o_am_conn_core,
            self.locator_id,
            self.length_c,
            self.acc_char_length as u64,
            &mut self.server_resource_consumption_info,
        )?;

        debug!("fetch_next_chunk(): got {} bytes", reply_data.len());

        self.acc_byte_length += reply_data.len();
        self.acc_char_length += util::count_1_2_3_sequence_starts(&reply_data);

        let (utf8, surrogate_buf) = match self.surrogate_buf {
            Some(ref buf) => {
                let mut temp = buf.to_vec();
                temp.append(&mut reply_data);
                util::to_string_and_surrogate(&temp).unwrap(/* yes */)
            }
            None => util::to_string_and_surrogate(&reply_data).unwrap(/* yes */),
        };

        self.utf8.push_str(&utf8);
        self.surrogate_buf = surrogate_buf;
        self.is_data_complete = reply_is_last_data;
        self.max_size = max(
            self.utf8.len() + if self.surrogate_buf.is_some() { 3 } else { 0 },
            self.max_size,
        );

        if self.is_data_complete {
            if self.length_b != self.acc_byte_length as u64 {
                error!(
                    "fetch_next_chunk(): is_data_complete = {}, length_c = {}, length_b = {}, \
                     locator_id = {}, surrogate_buf = {:?}, utf8.len() = {}",
                    self.is_data_complete,
                    self.length_c,
                    self.length_b,
                    self.locator_id,
                    self.surrogate_buf,
                    self.utf8.len()
                );
                trace!("utf8: {:?}", self.utf8);
            }
            assert_eq!(self.length_b, self.acc_byte_length as u64);
            debug!("max_size: {}", self.max_size);
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

    /// Converts a NCLobHandle into a String containing its data.
    pub fn into_string(mut self) -> HdbResult<String> {
        trace!("into_string()");
        self.load_complete()?;
        Ok(self.utf8)
    }
}

// Support for CLOB streaming
impl io::Read for NCLobHandle {
    fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
        trace!("read() with buf of len {}", buf.len());

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
