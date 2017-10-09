use super::{PrtError, PrtResult, prot_err, util};

use byteorder::{LittleEndian, ReadBytesExt};
use std::fmt;
use std::io;
use std::u32;
use vec_map::VecMap;


/// contains a table of field metadata;
/// the variable-length Strings are extracted into the names vecmap, which uses an integer as key
#[derive(Clone,Debug)]
pub struct ResultSetMetadata {
    fields: Vec<FieldMetadata>,
    names: VecMap<String>,
}
impl ResultSetMetadata {
    /// Factory method for ResultSetMetadata, only useful for tests.
    #[allow(dead_code)]
    pub fn new_for_tests() -> ResultSetMetadata {
        ResultSetMetadata {
            fields: Vec::<FieldMetadata>::new(),
            names: VecMap::<String>::new(),
        }
    }

    /// Returns the number of fields (columns) in the ResultSet.
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    fn add_to_names(&mut self, offset: u32) {
        if offset != u32::MAX {
            let tn = offset as usize;
            if !self.names.contains_key(tn) {
                self.names.insert(tn, "".to_string());
            };
        }
    }

    /// Returns the number of described fields.
    pub fn count(&self) -> i16 {
        self.fields.len() as i16
    }

    /// Returns the metadata of a specified field, or None if the index is too big.
    pub fn get_fieldmetadata(&self, field_idx: usize) -> Option<&FieldMetadata> {
        self.fields.get(field_idx)
    }

    /// FIXME for large resultsets, this method will be called very often - is caching meaningful?
    #[inline]
    pub fn get_fieldname(&self, field_idx: usize) -> Option<&String> {
        match self.fields.get(field_idx) {
            Some(field_metadata) => self.names.get(field_metadata.column_displayname as usize),
            None => None,
        }
    }
}


pub fn parse(count: i32, arg_size: u32, rdr: &mut io::BufRead) -> PrtResult<ResultSetMetadata> {
    let mut rsm = ResultSetMetadata {
        fields: Vec::<FieldMetadata>::new(),
        names: VecMap::<String>::new(),
    };
    trace!("Got count {}", count);
    for _ in 0..count {
        let co = rdr.read_u8()?; // U1 (documented as I1)
        let vt = rdr.read_u8()?; // I1
        let fr = rdr.read_i16::<LittleEndian>()?; // I2
        let pr = rdr.read_i16::<LittleEndian>()?; // I2
        rdr.read_i16::<LittleEndian>()?; // I2
        let tn = rdr.read_u32::<LittleEndian>()?; // I4
        rsm.add_to_names(tn);
        let sn = rdr.read_u32::<LittleEndian>()?; // I4
        rsm.add_to_names(sn);
        let cn = rdr.read_u32::<LittleEndian>()?; // I4
        rsm.add_to_names(cn);
        let cdn = rdr.read_u32::<LittleEndian>()?; // I4
        rsm.add_to_names(cdn);

        let fm = FieldMetadata::new(co, vt, fr, pr, tn, sn, cn, cdn)?;
        rsm.fields.push(fm);
    }
    trace!("Read ResultSetMetadata phase 1: {:?}", rsm);
    // now we read the names
    let mut offset = 0;
    let limit = arg_size - (count as u32) * 22;
    trace!("arg_size = {}, count = {}, limit = {} ", arg_size, count, limit);
    for _ in 0..rsm.names.len() {
        if offset >= limit {
            return Err(prot_err("Error in reading ResultSetMetadata"));
        };
        let nl = rdr.read_u8()?; // UI1
        let buffer: Vec<u8> = util::parse_bytes(nl as usize, rdr)?; // variable
        let name = util::cesu8_to_string(&buffer)?;
        trace!("offset = {}, name = {}", offset, name);
        rsm.names.insert(offset as usize, name);
        offset += (nl as u32) + 1;
    }
    Ok(rsm)
}


/// Describes a single field (column) in a result set.
#[derive(Clone,Debug)]
pub struct FieldMetadata {
    /// Database schema.
    pub schemaname: u32,
    /// Database table.
    pub tablename: u32,
    /// Name of the column.
    pub columnname: u32,
    /// Various column settings.
    pub column_option: ColumnOption,
    /// The id of the value type.
    pub value_type: u8,
    /// Fraction length (for some numeric types only).
    pub fraction: i16,
    /// Precision (for some numeric types only).
    pub precision: i16,
    /// Display name of a column.
    pub column_displayname: u32,
}
impl FieldMetadata {
    /// Factory method for FieldMetadata; only usable for tests.
    pub fn new(co: u8, vt: u8, fr: i16, pr: i16, tn: u32, sn: u32, cn: u32, cdn: u32)
               -> PrtResult<FieldMetadata> {
        Ok(FieldMetadata {
            column_option: ColumnOption::from_u8(co)?,
            value_type: vt,
            fraction: fr,
            precision: pr,
            tablename: tn,
            schemaname: sn,
            columnname: cn,
            column_displayname: cdn,
        })
    }
}

#[derive(Clone,Debug)]
pub enum ColumnOption {
    Nullable,
    NotNull,
}
impl ColumnOption {
    pub fn is_nullable(&self) -> bool {
        match *self {
            ColumnOption::Nullable => true,
            ColumnOption::NotNull => false,
        }
    }

    fn from_u8(val: u8) -> PrtResult<ColumnOption> {
        match val {
            1 => Ok(ColumnOption::NotNull),
            2 => Ok(ColumnOption::Nullable),
            _ => {
                Err(PrtError::ProtocolError(format!("ColumnOption::from_u8() not implemented \
                                                     for value {}",
                                                    val)))
            }
        }
    }
}

// this just writes a headline with field names as it is handy in Display for ResultSet
impl fmt::Display for ResultSetMetadata {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        writeln!(fmt, "").unwrap();
        for field_metadata in &self.fields {
            match self.names.get(field_metadata.column_displayname as usize) {
                Some(fieldname) => write!(fmt, "{}, ", fieldname).unwrap(),
                None => write!(fmt, "<unnamed>, ").unwrap(),
            };
        }
        Ok(())
    }
}
