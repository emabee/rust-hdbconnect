use crate::protocol::parts::type_id::TypeId;
use crate::protocol::util;

use byteorder::{LittleEndian, ReadBytesExt};
use std::rc::Rc;
use vec_map::VecMap;

pub(crate) type ResultSetMetadata = Vec<FieldMetadata>;

pub(crate) fn parse_resultset_metadata(
    count: usize,
    rdr: &mut dyn std::io::Read,
) -> std::io::Result<Vec<FieldMetadata>> {
    fn add_to_names(names: &mut VecMap<String>, offset: u32) {
        if offset != u32::max_value() {
            let offset = offset as usize;
            if !names.contains_key(offset) {
                names.insert(offset, "".to_string());
            };
        }
    }

    let mut inner_fms = Vec::<InnerFieldMetadata>::new();
    let mut names = VecMap::<String>::new();

    trace!("Got count {}", count);
    for _ in 0..count {
        let column_options = rdr.read_u8()?; // U1 (documented as I1)
        let type_code = rdr.read_u8()?; // I1
        let scale = rdr.read_i16::<LittleEndian>()?; // I2
        let precision = rdr.read_i16::<LittleEndian>()?; // I2
        rdr.read_i16::<LittleEndian>()?; // I2
        let tablename_idx = rdr.read_u32::<LittleEndian>()?; // I4
        add_to_names(&mut names, tablename_idx);
        let schemaname_idx = rdr.read_u32::<LittleEndian>()?; // I4
        add_to_names(&mut names, schemaname_idx);
        let columnname_idx = rdr.read_u32::<LittleEndian>()?; // I4
        add_to_names(&mut names, columnname_idx);
        let displayname_idx = rdr.read_u32::<LittleEndian>()?; // I4
        add_to_names(&mut names, displayname_idx);

        let type_id = TypeId::try_new(type_code)?;
        let fm = InnerFieldMetadata {
            schemaname_idx,
            tablename_idx,
            columnname_idx,
            displayname_idx,
            column_options,
            type_id,
            scale,
            precision,
        };
        inner_fms.push(fm);
    }
    // now we read the names
    let mut offset = 0;
    for _ in 0..names.len() {
        let nl = rdr.read_u8()?; // UI1
        let name = util::string_from_cesu8(util::parse_bytes(nl as usize, rdr)?)
            .map_err(util::io_error)?; // variable
        trace!("offset = {}, name = {}", offset, name);
        names.insert(offset as usize, name.to_string());
        offset += u32::from(nl) + 1;
    }

    let names = Rc::new(names);

    Ok(inner_fms
        .into_iter()
        .map(|inner| FieldMetadata {
            inner,
            names: Rc::clone(&names),
        })
        .collect())
}

/// Metadata of a field in a `ResultSet`.
#[derive(Clone, Debug)]
pub struct FieldMetadata {
    inner: InnerFieldMetadata,
    names: Rc<VecMap<String>>,
}

/// Describes a single field (column) in a result set.
#[derive(Clone, Copy, Debug)]
struct InnerFieldMetadata {
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
    // scale
    scale: i16,
    // Precision
    precision: i16,
}

impl FieldMetadata {
    /// Database schema of the field.
    pub fn schemaname(&self) -> &str {
        self.names
            .get(self.inner.schemaname_idx as usize)
            .map_or("", String::as_str)
    }

    /// Database table.
    pub fn tablename(&self) -> &str {
        self.names
            .get(self.inner.tablename_idx as usize)
            .map_or("", String::as_str)
    }

    /// Column name.
    pub fn columnname(&self) -> &str {
        self.names
            .get(self.inner.columnname_idx as usize)
            .map_or("", String::as_str)
    }

    /// Display name of the column.
    pub fn displayname(&self) -> &str {
        self.names
            .get(self.inner.displayname_idx as usize)
            .map_or("", String::as_str)
    }

    /// Returns the id of the value type.
    pub fn type_id(&self) -> TypeId {
        self.inner.type_id
    }

    /// True if column can contain NULL values.
    pub fn is_nullable(&self) -> bool {
        (self.inner.column_options & 0b_0000_0010_u8) != 0
    }

    /// The length or the precision of the value.
    ///
    /// Is `-1` for LOB types.
    pub fn precision(&self) -> i16 {
        self.inner.precision
    }

    /// The scale of the value.
    ///
    /// Is `0` for all types where a scale does not make sense.
    pub fn scale(&self) -> i16 {
        self.inner.scale
    }

    /// Returns true if the column has a default value.
    pub fn has_default(&self) -> bool {
        (self.inner.column_options & 0b_0000_0100_u8) != 0
    }

    ///  Returns true if the column is read-only.
    pub fn is_read_only(&self) -> bool {
        (self.inner.column_options & 0b_0100_0000_u8) != 0
    }

    /// Returns true if the column is auto-incremented.
    pub fn is_auto_incremented(&self) -> bool {
        (self.inner.column_options & 0b_0010_0000_u8) != 0
    }

    /// Returns true if the column is of array type.
    pub fn is_array_type(&self) -> bool {
        (self.inner.column_options & 0b_0100_0000_u8) != 0
    }
}
