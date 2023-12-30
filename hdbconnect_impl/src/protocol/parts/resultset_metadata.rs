use crate::{
    protocol::{parts::field_metadata::InnerFieldMetadata, util, util_sync},
    FieldMetadata, HdbResult, TypeId,
};
use byteorder::{LittleEndian, ReadBytesExt};
use std::{ops::Deref, sync::Arc};
use vec_map::VecMap;

/// List of metadata of the fields of a resultset.
///
/// Provides index access to the `FieldMetadata` of the individual fields.
///
/// ## Example
///
/// ```rust
/// use hdbconnect::ResultSet;
/// let resultset: ResultSet;
/// let metadata = resultset.metadata();  // ResultSetMetadata
///
/// // direct access to field properties
/// let scale_0 = metadata[0].scale();
/// let type_id_0 = metadata[0].type_id();
///
/// // all metadata of a single fields
/// let field_1 = metadata[1]; // FieldMetadata
/// let precision_1 = metadata[1].precision();
/// ```
#[derive(Debug)]
pub struct ResultSetMetadata(Vec<FieldMetadata>);
impl Deref for ResultSetMetadata {
    type Target = Vec<FieldMetadata>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::fmt::Display for ResultSetMetadata {
    // Writes a header and then the data
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(fmt)?;
        for field_metadata in &self.0 {
            write!(fmt, "{}, ", field_metadata.displayname())?;
        }
        writeln!(fmt)?;
        Ok(())
    }
}

impl ResultSetMetadata {
    pub(crate) fn parse(count: usize, rdr: &mut dyn std::io::Read) -> HdbResult<Self> {
        let mut inner_fms = Vec::<InnerFieldMetadata>::new();
        let mut names = VecMap::<String>::new();

        trace!("ResultSetMetadata::parse_sync: Got count = {count}");
        for _ in 0..count {
            let column_options = rdr.read_u8()?;
            let type_code = rdr.read_u8()?;
            let scale = rdr.read_i16::<LittleEndian>()?;
            let precision = rdr.read_i16::<LittleEndian>()?;
            rdr.read_i16::<LittleEndian>()?;
            let tablename_idx = rdr.read_u32::<LittleEndian>()?;
            add_to_names(&mut names, tablename_idx);
            let schemaname_idx = rdr.read_u32::<LittleEndian>()?;
            add_to_names(&mut names, schemaname_idx);
            let columnname_idx = rdr.read_u32::<LittleEndian>()?;
            add_to_names(&mut names, columnname_idx);
            let displayname_idx = rdr.read_u32::<LittleEndian>()?;
            add_to_names(&mut names, displayname_idx);

            let type_id = TypeId::try_new(type_code)?;
            inner_fms.push(InnerFieldMetadata::new(
                schemaname_idx,
                tablename_idx,
                columnname_idx,
                displayname_idx,
                column_options,
                type_id,
                scale,
                precision,
            ));
        }
        // now we read the names
        let mut offset = 0;
        for _ in 0..names.len() {
            let nl = rdr.read_u8()?;
            let name = util::string_from_cesu8(util_sync::parse_bytes(nl as usize, rdr)?)
                .map_err(util::io_error)?;
            trace!("offset = {offset}, name = {name}");
            names.insert(offset as usize, name);
            offset += u32::from(nl) + 1;
        }

        let names = Arc::new(names);

        Ok(ResultSetMetadata(
            inner_fms
                .into_iter()
                .map(|inner| FieldMetadata::new(inner, Arc::clone(&names)))
                .collect(),
        ))
    }
}

fn add_to_names(names: &mut VecMap<String>, offset: u32) {
    if offset != u32::max_value() {
        let offset = offset as usize;
        if !names.contains_key(offset) {
            names.insert(offset, String::new());
        };
    }
}
