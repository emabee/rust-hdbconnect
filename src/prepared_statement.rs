use {DbcError,DbcResult,DbResponse};
use protocol::lowlevel::conn_core::ConnRef;
use protocol::lowlevel::argument::Argument;
use protocol::lowlevel::message::{Request,Metadata};
use protocol::lowlevel::request_type::RequestType;
use protocol::lowlevel::part::Part;
use protocol::lowlevel::partkind::PartKind;
use protocol::lowlevel::parts::parameter_metadata::ParameterMetadata;
use protocol::lowlevel::parts::parameters::{ParameterRow,Parameters};
use rs_serde::error::SerializationError;
use rs_serde::serializer::Serializer;

use serde;

use std::mem;

pub struct PreparedStatement {
    conn_ref: ConnRef,
    statement_id: u64,
    auto_commit: bool,
    par_md: Option<ParameterMetadata>, // optional, because there will not always be parameters
    batch: Option<Vec<ParameterRow>>,
}

impl PreparedStatement {
    /// Prepare a statement
    pub fn prepare(conn_ref: ConnRef, stmt: String,) -> DbcResult<PreparedStatement> {
        let auto_commit: bool = {conn_ref.borrow().auto_commit};
        let command_options: u8 = 8;
        let mut request = try!(Request::new(&conn_ref, RequestType::Prepare, auto_commit, command_options));
        request.push(Part::new(PartKind::Command, Argument::Command(stmt)));

        let mut reply = try!(request.send_and_receive(&conn_ref, None));

        let statement_id = match try!(reply.retrieve_first_part_of_kind(PartKind::StatementId)).arg {
            Argument::StatementId(statement_id) => statement_id,
            _ => return Err(DbcError::EvaluationError(String::from("No StatementId in PreparedStatement::prepare()"))),
        };
        let par_md = match try!(reply.retrieve_first_part_of_kind(PartKind::ParameterMetadata)).arg {
            Argument::ParameterMetadata(par_md) => Some(par_md),
            _ => None,
        };

        Ok(PreparedStatement {
            conn_ref: conn_ref,
            statement_id: statement_id,
            auto_commit: auto_commit,
            batch: match &par_md {&Some(_) => Some(Vec::<ParameterRow>::new()), &None => None},
            par_md: par_md,
        })
    }

    /// transmute the rust-typed from_row into a ParameterRow and add it to the batch;
    /// ensure that the Row is consistent to the metadata
    pub fn add_batch<T>(&mut self, from_row: &T) -> DbcResult<()>
    where T: serde::ser::Serialize
    {
        match (&(self.par_md), &mut (self.batch)) {
            (&Some(ref metadata), &mut Some(ref mut vec)) => {
                let row = try!(Serializer::into_row(from_row, &metadata));
                vec.push(row);
                Ok(())
            },
            (_,_) => Err(DbcError::SerializationError(SerializationError::StructuralMismatch("no metadata in add_batch()")))
        }
    }

    // Execute the statement with the collected batch, and clear the batch
    pub fn execute_batch(&mut self) -> DbcResult<DbResponse> {
        let mut request = try!(Request::new( &(self.conn_ref), RequestType::Execute, self.auto_commit, 8_u8));
        request.push(Part::new(PartKind::StatementId, Argument::StatementId(self.statement_id.clone())));
        if let &mut Some(ref mut pars1) = &mut self.batch {
            let mut pars2 = Vec::<ParameterRow>::new();
            mem::swap(pars1,&mut pars2);
            debug!("pars: {:?}",pars2);
            request.push(Part::new(PartKind::Parameters, Argument::Parameters(Parameters::new(pars2))));
        }
        let metadata = match self.par_md {
            Some(ref md) => Metadata::ParameterMetadata(&md),
            None => Metadata::None,
        };
        request.send_and_get_response(metadata, &(self.conn_ref), None)
    }
}


impl Drop for PreparedStatement {
    fn drop(&mut self) {
        match Request::new( &(self.conn_ref), RequestType::DropStatementId, false, 0) {
            Err(_) => {},
            Ok(mut request) => {
                request.push(Part::new(PartKind::StatementId, Argument::StatementId(self.statement_id.clone())));
                request.send_and_receive(&(self.conn_ref), None).ok();
    }}}
}
