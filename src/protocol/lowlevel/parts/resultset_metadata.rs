use HdbResult;
use super::{prot_err, util, PrtError, PrtResult};
use byteorder::{LittleEndian, ReadBytesExt};
use std::fmt;
use std::io;
use std::u32;
use vec_map::VecMap;

/// Metadata for the fields in a result set.
#[derive(Clone, Debug)]
pub struct ResultSetMetadata {
    fields: Vec<FieldMetadata>,
    names: VecMap<String>,
}
impl ResultSetMetadata {
    /// Factory method for ResultSetMetadata, only useful for tests.
    #[allow(dead_code)]
    #[doc(hidden)]
    pub fn new_for_tests() -> ResultSetMetadata {
        ResultSetMetadata {
            fields: Vec::<FieldMetadata>::new(),
            names: VecMap::<String>::new(),
        }
    }

    /// Returns the number of fields.
    pub fn number_of_fields(&self) -> usize {
        self.fields.len()
    }

    /// Returns true if the set of fields is empty.
    pub fn is_empty(&self) -> bool {
        self.fields.len() == 0
    }

    fn add_to_names(&mut self, offset: u32) {
        if offset != u32::MAX {
            let tn = offset as usize;
            if !self.names.contains_key(tn) {
                self.names.insert(tn, "".to_string());
            };
        }
    }

    fn get(&self, index: usize) -> PrtResult<&FieldMetadata> {
        self.fields
            .get(index)
            .ok_or(PrtError::UsageError("schemaname(): invalid field index"))
    }

    /// Database schema of the i'th column in the resultset.
    pub fn schemaname(&self, i: usize) -> HdbResult<&String> {
        Ok(self.names
            .get(self.get(i)?.schemaname_idx() as usize)
            .ok_or(PrtError::UsageError("get_fieldname(): invalid field index"))?)
    }

    /// Database table of the i'th column in the resultset.
    pub fn tablename(&self, i: usize) -> HdbResult<&String> {
        Ok(self.names
            .get(self.get(i)?.tablename_idx() as usize)
            .ok_or(PrtError::UsageError("tablename(): invalid field index"))?)
    }

    /// Name of the i'th column in the resultset.
    pub fn columnname(&self, i: usize) -> HdbResult<&String> {
        Ok(self.names
            .get(self.get(i)?.columnname_idx() as usize)
            .ok_or(PrtError::UsageError("columnname(): invalid field index"))?)
    }

    // For large resultsets, this method will be called very often - is caching meaningful?
    /// Display name of the column.
    #[inline]
    pub fn displayname(&self, index: usize) -> HdbResult<&String> {
        Ok(self.names
            .get(self.get(index)?.displayname_idx() as usize)
            .ok_or(PrtError::UsageError("get_fieldname(): invalid field index"))?)
    }

    /// True if column can contain NULL values.
    pub fn nullable(&self, i: usize) -> HdbResult<bool> {
        Ok(self.get(i)?.nullable())
    }

    /// Returns the id of the value type. See module `hdbconnect::metadata::type_id`.
    pub fn type_id(&self, i: usize) -> HdbResult<u8> {
        Ok(self.get(i)?.type_id())
    }

    /// Scale length (for some numeric types only).
    pub fn scale(&self, i: usize) -> HdbResult<i16> {
        Ok(self.get(i)?.scale())
    }

    /// Precision (for some numeric types only).
    pub fn precision(&self, i: usize) -> HdbResult<i16> {
        Ok(self.get(i)?.precision())
    }
}

// this just writes a headline with field names as it is handy in Display for ResultSet
impl fmt::Display for ResultSetMetadata {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        writeln!(fmt, "").unwrap();
        for field_metadata in &self.fields {
            match self.names.get(field_metadata.displayname_idx as usize) {
                Some(fieldname) => write!(fmt, "{}, ", fieldname).unwrap(),
                None => write!(fmt, "<unnamed>, ").unwrap(),
            };
        }
        Ok(())
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
        let sc = rdr.read_i16::<LittleEndian>()?; // I2
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

        let fm = FieldMetadata {
            nullable: Nullable::from_u8(co)?,
            type_id: vt,
            scale: sc,
            precision: pr,
            tablename_idx: tn,
            schemaname_idx: sn,
            columnname_idx: cn,
            displayname_idx: cdn,
        };
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
        offset += u32::from(nl) + 1;
    }
    Ok(rsm)
}


/// Describes a single field (column) in a result set.
#[derive(Clone, Debug)]
struct FieldMetadata {
    // Database schema.
    schemaname_idx: u32,
    // Database table.
    tablename_idx: u32,
    // Name of the column.
    columnname_idx: u32,
    // Display name of a column.
    displayname_idx: u32,
    // Whether the column can have NULL values.
    nullable: Nullable,
    // The id of the value type.
    type_id: u8,
    // scale length (for some numeric types only).
    scale: i16,
    // Precision (for some numeric types only).
    precision: i16,
}
impl FieldMetadata {
    /// Database schema.
    pub fn schemaname_idx(&self) -> u32 {
        self.schemaname_idx
    }
    /// Database table.
    pub fn tablename_idx(&self) -> u32 {
        self.tablename_idx
    }
    /// Name of the column.
    pub fn columnname_idx(&self) -> u32 {
        self.columnname_idx
    }
    /// Display name of a column.
    pub fn displayname_idx(&self) -> u32 {
        self.displayname_idx
    }
    /// Various column settings.
    pub fn nullable(&self) -> bool {
        self.nullable.0
    }
    /// The id of the value type.
    pub fn type_id(&self) -> u8 {
        self.type_id
    }
    /// Scale (for some numeric types only).
    pub fn scale(&self) -> i16 {
        self.scale
    }
    /// Precision (for some numeric types only).
    pub fn precision(&self) -> i16 {
        self.precision
    }
}

/// Describes whether the column can have NULL values.
#[derive(Clone, Debug)]
struct Nullable(bool);

impl Nullable {
    fn from_u8(val: u8) -> PrtResult<Nullable> {
        match val {
            1 => Ok(Nullable(false)),
            2 => Ok(Nullable(true)),
            _ => Err(PrtError::ProtocolError(
                format!("ColumnOption::from_u8() not implemented for value {}", val),
            )),
        }
    }
}
