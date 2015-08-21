use super::part::PartKind;

use byteorder::{/*BigEndian, */LittleEndian, WriteBytesExt};
use std::io::Result as IoResult;
use std::io::Write;

use std::fmt::{Debug, Error, Formatter};

const TYPE_CODE_STRING: i8 = 29;  // api/Communication/Protocol/Layout.hpp


pub fn new(part_kind: PartKind) -> Argument {
    match part_kind {
        PartKind::ConnectOptions |
        PartKind::CommitOptions |
        PartKind::FetchOptions => {Argument::HdbOptions(Vec::<HdbOption>::new())},
        PartKind::Authentication => {Argument::Auth(AuthS::new())},
        _ => {panic!("deserialization of argument for PartKind {} is not yet implemented", part_kind.to_i8())}
    }
}

#[derive(Debug)]
pub enum Argument {
    HdbOptions(Vec<HdbOption>),
    Auth(AuthS),
}

impl Argument {
    pub fn count(&self) -> i16 {
        match *self {
            Argument::HdbOptions(ref opts) => opts.len() as i16,
            Argument::Auth(_) => 1i16,
        }
    }

    pub fn size(&self, with_padding: bool) -> u32 {
        let mut size = 0;
        match self {
            &Argument::HdbOptions(ref opts) => {
                for opt in opts {
                    size += 1 + 1 + 2 + opt.value.len() as u32;
                }
            },
            &Argument::Auth(ref auth) => {
                size = 3 + auth.user.len() as u32;
                let ref ams = auth.methods;
                for ref am in ams {
                    size += 1 + am.name.len() as u32 + 1 + am.client_challenge.len() as u32;
                }
            }
        }
        if with_padding {
            size += padsize(size);
        }
        trace!("Part_buffer_size = {}",size);
        size
    }

    /// Serialize to byte stream
    pub fn encode(&self, remaining_bufsize: u32, w: &mut Write) -> IoResult<u32> {
        match *self {
            Argument::HdbOptions(ref opts) => {
                for ref opt in opts {
                    try!(w.write_i8(opt.id.getval()));                          // I1           OPTION KEY
                    // FIXME: support more than only strings
                    try!(w.write_i8(TYPE_CODE_STRING));                         // I1           TYPE OF OPTION VALUE
                    try!(w.write_i16::<LittleEndian>(opt.value.len() as i16));  // I2           LENGTH OF OPTION VALUE
                    for b in &(opt.value) {try!(w.write_u8(*b));}               // B variable   OPTION VALUE
                }
            },
            Argument::Auth(ref auth) => {
                let fieldcount = 1 + 2 * auth.methods.len() as i16;
                try!(w.write_i16::<LittleEndian>(fieldcount));                  // documented as I2BIGENDIAN!?!?  FIELD COUNT

                try!(encode_field(&(auth.user), w));                               // I1 + B variable   FIELD LENGTH and DATA

                let ref ams = auth.methods;
                for ref am in ams {
                    // method
                    try!(encode_field(&(am.name), w));                             // I1 + B variable   METHOD NAME
                    try!(encode_field(&(am.client_challenge), w));                 // I1 + B variable   CLIENT CHALLENGE
                }
            }
        }

        let size = self.size(false);
        let padsize = padsize(size);
        for _ in 0..padsize { try!(w.write_u8(0)); }

        Ok(remaining_bufsize - size - padsize)
    }
}

fn padsize(size: u32) -> u32 {
    7 - (size-1)%8
}

fn encode_field(f: &Vec<u8>, w: &mut Write) -> IoResult<()> {
    try!(w.write_u8(f.len() as u8));                               // I1           FIELD LENGTH
    for b in f { try!(w.write_u8(*b)); }                            // B variable   FIELD DATA
    Ok(())
}

#[derive(Debug)]
pub struct AuthS {
    pub user: Vec<u8>,
    pub methods: Vec<AuthMethod>,
}
impl AuthS {
    fn new() -> AuthS {
        AuthS {user: Vec::<u8>::new(), methods: Vec::<AuthMethod>::new()}
    }
}


pub struct AuthMethod {
    pub name: Vec<u8>,
    pub client_challenge: Vec<u8>,
}
impl Debug for AuthMethod {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        try!(write!(f, "AuthMethod{{name: \"{}\",", String::from_utf8(self.name.clone()).unwrap()));
        try!(write!(f, "clientchallenge: \""));
        for b in &(self.client_challenge) {
            try!(write!(f, "{:0>2X}", b));
        }
        try!(write!(f, "\"}}"));
        Ok(())
    }
}

#[derive(Debug)]
pub struct HdbOption {
    pub id: HdbOptionId,
    pub value: Vec<u8>,
}

#[derive(Debug)]
pub enum HdbOptionId {
    Version,
    ClientType,
    ClientApplicationProgram,
}
impl HdbOptionId {
    fn getval(&self) -> i8 {
        match *self {
            HdbOptionId::Version => 1,
            HdbOptionId::ClientType => 2,
            HdbOptionId::ClientApplicationProgram => 3,
        }
    }
}
