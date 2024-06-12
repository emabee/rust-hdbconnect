use crate::{
    conn::AmConnCore,
    protocol::{
        parts::{
            hdb_value::HdbValue,
            parameter_descriptor::{ParameterDescriptor, ParameterDescriptors},
        },
        util,
    },
    serde_db_impl::de::DeserializableOutputParameters,
    HdbResult,
};
use serde_db::de::DeserializableRow;

/// A set of output parameters, as they can be returned by procedure calls.
///
/// Contains metadata (the descriptors), and the values.
///
///
#[derive(Debug)]
pub struct OutputParameters {
    descriptors: Vec<ParameterDescriptor>,
    values: Vec<HdbValue<'static>>,
}

impl OutputParameters {
    /// Converts the contained values in into a plain rust value or a tuple, etc.
    ///
    /// # Errors
    ///
    /// `HdbError::Deserialization` if the conversion is not implemented.
    pub fn try_into<'de, T>(self) -> HdbResult<T>
    where
        T: serde::de::Deserialize<'de>,
    {
        trace!("OutputParameters::into_typed()");
        Ok(DeserializableRow::try_into(
            DeserializableOutputParameters::new(self),
        )?)
    }

    /// Returns the descriptors.
    pub fn descriptors(&self) -> &Vec<ParameterDescriptor> {
        &(self.descriptors)
    }

    /// Converts into an iterator of the contained values.
    pub fn into_values(self) -> Vec<HdbValue<'static>> {
        self.values
    }

    /// Converts into a vec of the parameter descriptors and a vec of the contained values.
    pub fn into_descriptors_and_values(self) -> (Vec<ParameterDescriptor>, Vec<HdbValue<'static>>) {
        (self.descriptors, self.values)
    }

    /// Exposes a vec of the parameter descriptors and a vec of the contained values.
    pub fn as_descriptors_and_values(
        &self,
    ) -> (&Vec<ParameterDescriptor>, &Vec<HdbValue<'static>>) {
        (&self.descriptors, &self.values)
    }

    #[cfg(feature = "sync")]
    pub(crate) fn parse_sync(
        o_am_conn_core: Option<&AmConnCore>,
        parameter_descriptors: &ParameterDescriptors,
        rdr: &mut std::io::Cursor<Vec<u8>>,
    ) -> HdbResult<Self> {
        trace!("OutputParameters::parse()");
        let am_conn_core = o_am_conn_core
            .ok_or_else(|| util::io_error("Cannot parse output parameters without am_conn_core"))?;

        let mut descriptors = Vec::<ParameterDescriptor>::new();
        let mut values = Vec::<HdbValue<'static>>::new();

        for descriptor in parameter_descriptors.iter_out() {
            trace!("Parsing value with descriptor {}", descriptor);
            let value = HdbValue::parse_sync(
                descriptor.type_id(),
                descriptor.is_array_type(),
                descriptor.scale(),
                descriptor.is_nullable(),
                am_conn_core,
                &None,
                rdr,
            )?;
            trace!("Found value {:?}", value);
            descriptors.push(descriptor.clone());
            values.push(value);
        }
        Ok(Self {
            descriptors,
            values,
        })
    }

    #[cfg(feature = "async")]
    pub(crate) async fn parse_async(
        o_am_conn_core: Option<&AmConnCore>,
        parameter_descriptors: &ParameterDescriptors,
        rdr: &mut std::io::Cursor<Vec<u8>>,
    ) -> HdbResult<Self> {
        trace!("OutputParameters::parse()");
        let am_conn_core = o_am_conn_core
            .ok_or_else(|| util::io_error("Cannot parse output parameters without am_conn_core"))?;

        let mut descriptors = Vec::<ParameterDescriptor>::new();
        let mut values = Vec::<HdbValue<'static>>::new();

        for descriptor in parameter_descriptors.iter_out() {
            trace!("Parsing value with descriptor {}", descriptor);
            let value = HdbValue::parse_async(
                descriptor.type_id(),
                descriptor.is_array_type(),
                descriptor.scale(),
                descriptor.is_nullable(),
                am_conn_core,
                &None,
                rdr,
            )
            .await?;
            trace!("Found value {:?}", value);
            descriptors.push(descriptor.clone());
            values.push(value);
        }
        Ok(Self {
            descriptors,
            values,
        })
    }
}

impl std::fmt::Display for OutputParameters {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        // write a header
        writeln!(fmt)?;
        for parameter_descriptor in &self.descriptors {
            write!(fmt, "{}, ", parameter_descriptor.name().unwrap_or(""))?;
        }
        writeln!(fmt)?;

        // write the data
        for value in &self.values {
            write!(fmt, "{value}, ")?;
        }
        writeln!(fmt)?;
        Ok(())
    }
}
