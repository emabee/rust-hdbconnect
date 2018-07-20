///////////////////////////////////////////////////////////////
use protocol::lowlevel::argument::Argument;
use protocol::lowlevel::conn_core::AmConnCore;
use protocol::lowlevel::message::{Request, SkipLastSpace};
use protocol::lowlevel::part::Part;
use protocol::lowlevel::partkind::PartKind;
use protocol::lowlevel::reply_type::ReplyType;
use protocol::lowlevel::request_type::RequestType;
use {HdbError, HdbResult};

use std::cmp;
use std::io::{self, Write};
use std::sync::Arc;

/// `BlobHandle` is used for BLOBs that we receive from the database.
/// The data are often not transferred completely,
/// so we carry internally a database connection and the
/// necessary controls to support fetching remaining data on demand.
#[derive(Clone, Debug)]
pub struct BlobHandle {
    o_am_conn_core: Option<AmConnCore>,
    is_data_complete: bool,
    length_b: u64,
    locator_id: u64,
    data: Vec<u8>,
    max_size: usize,
    acc_byte_length: usize,
    acc_server_proc_time: i32,
}
impl BlobHandle {
    pub fn new(
        am_conn_core: &AmConnCore,
        is_data_complete: bool,
        length_b: u64,
        locator_id: u64,
        data: Vec<u8>,
    ) -> BlobHandle {
        trace!(
            "BlobHandle::new() with length_b = {}, is_data_complete = {}, data.length() = {}",
            length_b,
            is_data_complete,
            data.len()
        );
        BlobHandle {
            o_am_conn_core: Some(Arc::clone(am_conn_core)),
            length_b,
            is_data_complete,
            locator_id,
            max_size: data.len(),
            acc_byte_length: data.len(),
            data,
            acc_server_proc_time: 0,
        }
    }

    pub fn len_alldata(&mut self) -> u64 {
        self.length_b
    }

    pub fn len_readdata(&mut self) -> usize {
        self.data.len()
    }

    fn fetch_next_chunk(&mut self) -> HdbResult<()> {
        let (mut reply_data, reply_is_last_data, server_processing_time) = fetch_a_lob_chunk(
            &mut self.o_am_conn_core,
            self.locator_id,
            self.length_b,
            self.acc_byte_length as u64,
        )?;

        self.acc_byte_length += reply_data.len();
        self.data.append(&mut reply_data);
        self.is_data_complete = reply_is_last_data;
        self.acc_server_proc_time += server_processing_time;
        self.max_size = cmp::max(self.data.len(), self.max_size);

        assert_eq!(
            self.is_data_complete,
            self.length_b == self.acc_byte_length as u64
        );
        trace!(
            "After BlobHandle fetch: is_data_complete = {}, data.len() = {}",
            self.is_data_complete,
            self.data.len()
        );
        Ok(())
    }

    /// Converts a BLOB into a Vec<u8> containing its data.
    fn fetch_all(&mut self) -> HdbResult<()> {
        trace!("BlobHandle::fetch_all()");
        while !self.is_data_complete {
            self.fetch_next_chunk()?;
        }
        Ok(())
    }

    pub fn max_size(&self) -> usize {
        self.max_size
    }

    /// Converts a BLOB into a Vec<u8> containing its data.
    pub fn into_bytes(mut self) -> HdbResult<Vec<u8>> {
        trace!("BlobHandle::into_bytes()");
        self.fetch_all()?;
        Ok(self.data)
    }
}

// Support for streaming
impl io::Read for BlobHandle {
    fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
        trace!("BlobHandle::read() with buf of len {}", buf.len());

        while !self.is_data_complete && (buf.len() > self.data.len()) {
            self.fetch_next_chunk()
                .map_err(|e| io::Error::new(io::ErrorKind::UnexpectedEof, e))?;
        }

        let count = cmp::min(self.data.len(), buf.len());
        buf.write_all(&self.data[0..count])?;
        self.data.drain(0..count);
        Ok(count)
    }
}

#[derive(Debug)]
pub struct ReadLobRequest {
    locator_id: u64,
    offset: u64,
    length_to_read: i32,
}
impl ReadLobRequest {
    pub fn locator_id(&self) -> u64 {
        self.locator_id
    }
    pub fn offset(&self) -> u64 {
        self.offset
    }
    pub fn length_to_read(&self) -> i32 {
        self.length_to_read
    }
}
pub fn fetch_a_lob_chunk(
    o_am_conn_core: &mut Option<AmConnCore>,
    locator_id: u64,
    length_b: u64,
    data_len: u64,
) -> HdbResult<(Vec<u8>, bool, i32)> {
    match *o_am_conn_core {
        None => Err(HdbError::Impl(
            "Fetching more LOB chunks is no more possible (connection already closed)".to_owned(),
        )),
        Some(ref mut am_conn_core) => {
            // build the request, provide StatementContext and length_to_read
            let mut request = Request::new(RequestType::ReadLob, 0);
            let length_to_read = {
                let guard = am_conn_core.lock()?;
                cmp::min((*guard).get_lob_read_length() as u64, length_b - data_len) as i32
            };
            let offset = data_len + 1;
            request.push(Part::new(
                PartKind::ReadLobRequest,
                Argument::ReadLobRequest(ReadLobRequest {
                    locator_id,
                    offset,
                    length_to_read,
                }),
            ));

            trace!(
                "Sending ReadLobRequest with offset = {} and length_to_read = {}",
                offset,
                length_to_read
            );

            let mut reply = request.send_and_receive(
                am_conn_core,
                Some(ReplyType::ReadLob),
                SkipLastSpace::No,
            )?;

            let (reply_data, reply_is_last_data) =
                match reply.parts.pop_arg_if_kind(PartKind::ReadLobReply) {
                    Some(Argument::ReadLobReply(read_lob_reply)) => {
                        if *read_lob_reply.locator_id() != locator_id {
                            return Err(HdbError::Impl(
                                "lob::fetch_a_lob_chunk(): locator ids do not match".to_owned(),
                            ));
                        }
                        read_lob_reply.into_data_and_last()
                    }
                    _ => return Err(HdbError::Impl("No ReadLobReply part found".to_owned())),
                };

            let server_processing_time = match reply
                .parts
                .pop_arg_if_kind(PartKind::StatementContext)
            {
                Some(Argument::StatementContext(stmt_ctx)) => stmt_ctx.get_server_processing_time(),
                None => 0,
                _ => {
                    return Err(HdbError::Impl(
                        "Inconsistent StatementContext part found for ResultSet".to_owned(),
                    ))
                }
            };
            Ok((reply_data, reply_is_last_data, server_processing_time))
        }
    }
}
