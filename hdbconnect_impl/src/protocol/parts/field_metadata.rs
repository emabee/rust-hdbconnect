use crate::protocol::parts::type_id::TypeId;
use std::sync::Arc;
use vec_map::VecMap;

// The structure is a bit weird; reason is that we want to retain the transfer format
// which seeks to avoid String duplication

/// Metadata of a field in a `ResultSet`.
#[derive(Clone, Debug)]
pub struct FieldMetadata {
    inner: InnerFieldMetadata,
    names: Arc<VecMap<String>>,
}

/// Describes a single field (column) in a result set.
#[derive(Clone, Copy, Debug)]
pub(crate) struct InnerFieldMetadata {
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
impl InnerFieldMetadata {
    pub fn new(
        schemaname_idx: u32,
        tablename_idx: u32,
        columnname_idx: u32,
        displayname_idx: u32,
        column_options: u8,
        type_id: TypeId,
        scale: i16,
        precision: i16,
    ) -> Self {
        Self {
            schemaname_idx,
            tablename_idx,
            columnname_idx,
            displayname_idx,
            column_options,
            type_id,
            scale,
            precision,
        }
    }
}

impl FieldMetadata {
    pub(crate) fn new(inner: InnerFieldMetadata, names: Arc<VecMap<String>>) -> Self {
        Self { inner, names }
    }

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

    // Returns true for BLOB, CLOB, and NCLOB, and false otherwise.
    pub(crate) fn is_lob(&self) -> bool {
        match self.inner.type_id {
            TypeId::BLOB | TypeId::CLOB | TypeId::NCLOB => true,
            _ => false,
        }
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
