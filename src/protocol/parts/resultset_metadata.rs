use crate::protocol::parts::type_id::{BaseTypeId, TypeId};
use crate::protocol::util;
use crate::{HdbError, HdbResult};

use byteorder::{LittleEndian, ReadBytesExt};
use cesu8;
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

    fn get(&self, index: usize) -> HdbResult<&FieldMetadata> {
        self.fields
            .get(index)
            .ok_or_else(|| HdbError::usage_("schemaname(): invalid field index"))
    }

    /// Database schema of the i'th column in the resultset.
    pub fn schemaname(&self, i: usize) -> HdbResult<&String> {
        Ok(self
            .names
            .get(self.get(i)?.schemaname_idx() as usize)
            .ok_or_else(|| HdbError::usage_("get_fieldname(): invalid field index"))?)
    }

    /// Database table of the i'th column in the resultset.
    pub fn tablename(&self, i: usize) -> HdbResult<&String> {
        Ok(self
            .names
            .get(self.get(i)?.tablename_idx() as usize)
            .ok_or_else(|| HdbError::usage_("tablename(): invalid field index"))?)
    }

    /// Name of the i'th column in the resultset.
    pub fn columnname(&self, i: usize) -> HdbResult<&String> {
        Ok(self
            .names
            .get(self.get(i)?.columnname_idx() as usize)
            .ok_or_else(|| HdbError::usage_("columnname(): invalid field index"))?)
    }

    // For large resultsets, this method will be called very often - is caching
    // meaningful?
    /// Display name of the column.
    #[inline]
    pub fn displayname(&self, index: usize) -> HdbResult<&String> {
        Ok(self
            .names
            .get(self.get(index)?.displayname_idx() as usize)
            .ok_or_else(|| HdbError::usage_("get_fieldname(): invalid field index"))?)
    }

    /// True if column can contain NULL values.
    pub fn is_nullable(&self, i: usize) -> HdbResult<bool> {
        Ok(self.get(i)?.is_nullable())
    }

    /// Returns true if the column has a default value.
    pub fn has_default(&self, i: usize) -> HdbResult<bool> {
        Ok(self.get(i)?.has_default())
    }
    // 3 = Escape_char
    // ???
    ///  Returns true if the column is readonly.
    pub fn is_readonly(&self, i: usize) -> HdbResult<bool> {
        Ok(self.get(i)?.is_readonly())
    }
    /// Returns true if the column is auto-incremented.
    pub fn is_auto_incremented(&self, i: usize) -> HdbResult<bool> {
        Ok(self.get(i)?.is_auto_incremented())
    }
    // 6 = ArrayType
    /// Returns true if the column is of array type.
    pub fn is_array_type(&self, i: usize) -> HdbResult<bool> {
        Ok(self.get(i)?.is_array_type())
    }

    /// Returns the id of the value type.
    pub fn type_id(&self, i: usize) -> HdbResult<&TypeId> {
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

    pub(crate) fn parse<T: io::BufRead>(count: i32, rdr: &mut T) -> HdbResult<ResultSetMetadata> {
        let mut rsm = ResultSetMetadata {
            fields: Vec::<FieldMetadata>::new(),
            names: VecMap::<String>::new(),
        };
        trace!("Got count {}", count);
        for _ in 0..count {
            let column_options = rdr.read_u8()?; // U1 (documented as I1)
            let type_id = rdr.read_u8()?; // I1
            let scale = rdr.read_i16::<LittleEndian>()?; // I2
            let precision = rdr.read_i16::<LittleEndian>()?; // I2
            rdr.read_i16::<LittleEndian>()?; // I2
            let tablename_idx = rdr.read_u32::<LittleEndian>()?; // I4
            rsm.add_to_names(tablename_idx);
            let schemaname_idx = rdr.read_u32::<LittleEndian>()?; // I4
            rsm.add_to_names(schemaname_idx);
            let columnname_idx = rdr.read_u32::<LittleEndian>()?; // I4
            rsm.add_to_names(columnname_idx);
            let displayname_idx = rdr.read_u32::<LittleEndian>()?; // I4
            rsm.add_to_names(displayname_idx);

            let nullable = (column_options & 0b_0000_0010_u8) != 0;
            let base_type_id = BaseTypeId::from(type_id);
            let fm = FieldMetadata {
                column_options,
                type_id: TypeId::new(base_type_id, nullable),
                scale,
                precision,
                tablename_idx,
                schemaname_idx,
                columnname_idx,
                displayname_idx,
            };
            rsm.fields.push(fm);
        }
        trace!("Read ResultSetMetadata phase 1: {:?}", rsm);
        // now we read the names
        let mut offset = 0;
        for _ in 0..rsm.names.len() {
            let nl = rdr.read_u8()?; // UI1
            let name = util::string_from_cesu8(util::parse_bytes(nl as usize, rdr)?)?; // variable
            trace!("offset = {}, name = {}", offset, name);
            rsm.names.insert(offset as usize, name.to_string());
            offset += u32::from(nl) + 1;
        }
        Ok(rsm)
    }
}

// this just writes a headline with field names as it is handy in Display for
// ResultSet
impl fmt::Display for ResultSetMetadata {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        writeln!(fmt).unwrap();
        for field_metadata in &self.fields {
            match self.names.get(field_metadata.displayname_idx as usize) {
                Some(fieldname) => write!(fmt, "{}, ", fieldname).unwrap(),
                None => write!(fmt, "<unnamed>, ").unwrap(),
            };
        }
        Ok(())
    }
}

/// Describes a single field (column) in a result set.
#[derive(Clone, Debug)]
struct FieldMetadata {
    schemaname_idx: u32,
    tablename_idx: u32,
    columnname_idx: u32,
    displayname_idx: u32,
    // Column_options.
    // Bit pattern:
    // 0 = Mandatory
    // 1 = Optional
    // 2 = Default
    // 3 = Escape_char
    // 4 = Readonly
    // 5 = Autoincrement
    // 6 = ArrayType
    column_options: u8,
    type_id: TypeId,
    // scale (for some numeric types only)
    scale: i16,
    // Precision (for some numeric types only)
    precision: i16,
}
impl FieldMetadata {
    // Database schema.
    pub fn schemaname_idx(&self) -> u32 {
        self.schemaname_idx
    }
    // Database table.
    pub fn tablename_idx(&self) -> u32 {
        self.tablename_idx
    }
    // Name of the column.
    pub fn columnname_idx(&self) -> u32 {
        self.columnname_idx
    }
    // Display name of a column.
    pub fn displayname_idx(&self) -> u32 {
        self.displayname_idx
    }
    // Returns true if the column can contain NULL values.
    pub fn is_nullable(&self) -> bool {
        (self.column_options & 0b_0000_0010_u8) != 0
    }
    // Returns true if the column has a default value.
    pub fn has_default(&self) -> bool {
        (self.column_options & 0b_0000_0100_u8) != 0
    }
    // 3 = Escape_char
    // ???
    //  Returns true if the column is readonly
    pub fn is_readonly(&self) -> bool {
        (self.column_options & 0b_0001_0000_u8) != 0
    }
    // Returns true if the column is auto-incremented.
    pub fn is_auto_incremented(&self) -> bool {
        (self.column_options & 0b_0010_0000_u8) != 0
    }
    // 6 = ArrayType
    pub fn is_array_type(&self) -> bool {
        (self.column_options & 0b_0100_0000_u8) != 0
    }

    /// The id of the value type.
    pub fn type_id(&self) -> &TypeId {
        &(self.type_id)
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
