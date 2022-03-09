use crate::protocol::parts::type_id::TypeId;
use crate::protocol::{util, util_sync};
use crate::{HdbError, HdbResult};

use byteorder::{LittleEndian, ReadBytesExt};
use vec_map::VecMap;

const INVALID_FIELD_INDEX: &str = "invalid field index";

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
        if offset != u32::max_value() {
            let tn = offset as usize;
            if !self.names.contains_key(tn) {
                self.names.insert(tn, "".to_string());
            };
        }
    }

    fn field_metadata(&self, index: usize) -> HdbResult<&FieldMetadata> {
        Ok(self
            .fields
            .get(index)
            .ok_or_else(|| HdbError::Usage(INVALID_FIELD_INDEX))?)
    }

    pub(crate) fn typeid_nullable_scale(&self, index: usize) -> HdbResult<(TypeId, bool, i16)> {
        let fmd = self.field_metadata(index)?;
        Ok((fmd.type_id, fmd.is_nullable(), fmd.scale))
    }

    /// Database schema of the i'th column in the resultset.
    ///
    /// # Errors
    ///
    /// `HdbError::Usage` if the index is invalid
    pub fn schemaname(&self, i: usize) -> HdbResult<&str> {
        Ok(self
            .names
            .get(self.field_metadata(i)?.schemaname_idx as usize)
            .ok_or_else(|| HdbError::Usage(INVALID_FIELD_INDEX))?)
    }

    /// Database table of the i'th column in the resultset.
    ///
    /// # Errors
    ///
    /// `HdbError::Usage` if the index is invalid
    pub fn tablename(&self, i: usize) -> HdbResult<&str> {
        Ok(self
            .names
            .get(self.field_metadata(i)?.tablename_idx as usize)
            .ok_or_else(|| HdbError::Usage(INVALID_FIELD_INDEX))?)
    }

    /// Name of the i'th column in the resultset.
    ///
    /// # Errors
    ///
    /// `HdbError::Usage` if the index is invalid
    pub fn columnname(&self, i: usize) -> HdbResult<&str> {
        Ok(self
            .names
            .get(self.field_metadata(i)?.columnname_idx as usize)
            .ok_or_else(|| HdbError::Usage(INVALID_FIELD_INDEX))?)
    }

    // todo For large resultsets, this method will be called very often - is caching
    // meaningful?
    /// Display name of the column.
    ///
    /// # Errors
    ///
    /// `HdbError::Usage` if the index is invalid
    #[inline]
    pub fn displayname(&self, index: usize) -> HdbResult<&str> {
        Ok(self
            .names
            .get(self.field_metadata(index)?.displayname_idx as usize)
            .ok_or_else(|| HdbError::Usage(INVALID_FIELD_INDEX))?)
    }

    /// True if column can contain NULL values.
    ///
    /// # Errors
    ///
    /// `HdbError::Usage` if the index is invalid
    pub fn nullable(&self, i: usize) -> HdbResult<bool> {
        Ok(self.field_metadata(i)?.is_nullable())
    }

    /// Returns true if the column has a default value.
    ///
    /// # Errors
    ///
    /// `HdbError::Usage` if the index is invalid
    pub fn has_default(&self, i: usize) -> HdbResult<bool> {
        Ok(self.field_metadata(i)?.has_default())
    }
    // 3 = Escape_char
    // ???
    ///  Returns true if the column is read-only.
    ///
    /// # Errors
    ///
    /// `HdbError::Usage` if the index is invalid
    pub fn read_only(&self, i: usize) -> HdbResult<bool> {
        Ok(self.field_metadata(i)?.read_only())
    }
    /// Returns true if the column is auto-incremented.
    ///
    /// # Errors
    ///
    /// `HdbError::Usage` if the index is invalid
    pub fn is_auto_incremented(&self, i: usize) -> HdbResult<bool> {
        Ok(self.field_metadata(i)?.is_auto_incremented())
    }
    // 6 = ArrayType
    /// Returns true if the column is of array type.
    ///
    /// # Errors
    ///
    /// `HdbError::Usage` if the index is invalid
    pub fn is_array_type(&self, i: usize) -> HdbResult<bool> {
        Ok(self.field_metadata(i)?.is_array_type())
    }

    /// Returns the id of the value type.
    ///
    /// # Errors
    ///
    /// `HdbError::Usage` if the index is invalid
    pub fn type_id(&self, i: usize) -> HdbResult<TypeId> {
        Ok(self.field_metadata(i)?.type_id)
    }

    /// Scale length (for some numeric types only).
    ///
    /// # Errors
    ///
    /// `HdbError::Usage` if the index is invalid
    pub fn scale(&self, i: usize) -> HdbResult<i16> {
        Ok(self.field_metadata(i)?.scale)
    }

    /// Precision (for some numeric types only).
    ///
    /// # Errors
    ///
    /// `HdbError::Usage` if the index is invalid
    pub fn precision(&self, i: usize) -> HdbResult<i16> {
        Ok(self.field_metadata(i)?.precision)
    }

    pub(crate) fn parse_sync(count: usize, rdr: &mut dyn std::io::Read) -> std::io::Result<Self> {
        let mut rsm = Self {
            fields: Vec::<FieldMetadata>::new(),
            names: VecMap::<String>::new(),
        };
        trace!("Got count {}", count);
        for _ in 0..count {
            let column_options = rdr.read_u8()?; // U1 (documented as I1)
            let type_code = rdr.read_u8()?; // I1
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

            let type_id = TypeId::try_new(type_code)?;
            let fm = FieldMetadata {
                column_options,
                type_id,
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
            let name = util::string_from_cesu8(util_sync::parse_bytes(nl as usize, rdr)?)
                .map_err(util::io_error)?; // variable
            trace!("offset = {}, name = {}", offset, name);
            rsm.names.insert(offset as usize, name.to_string());
            offset += u32::from(nl) + 1;
        }
        Ok(rsm)
    }
}

// this just writes a headline with field names as it is handy in Display for ResultSet
impl std::fmt::Display for ResultSetMetadata {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(fmt)?;
        for field_metadata in &self.fields {
            match self.names.get(field_metadata.displayname_idx as usize) {
                Some(fieldname) => write!(fmt, "{}, ", fieldname)?,
                None => write!(fmt, "<unnamed>, ")?,
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
    // Returns true if the column can contain NULL values.
    fn is_nullable(&self) -> bool {
        (self.column_options & 0b_0000_0010_u8) != 0
    }
    // Returns true if the column has a default value.
    fn has_default(&self) -> bool {
        (self.column_options & 0b_0000_0100_u8) != 0
    }
    // 3 = Escape_char
    // ???
    //  Returns true if the column is read-only
    fn read_only(&self) -> bool {
        (self.column_options & 0b_0001_0000_u8) != 0
    }
    // Returns true if the column is auto-incremented.
    fn is_auto_incremented(&self) -> bool {
        (self.column_options & 0b_0010_0000_u8) != 0
    }
    // 6 = ArrayType
    fn is_array_type(&self) -> bool {
        (self.column_options & 0b_0100_0000_u8) != 0
    }
}
