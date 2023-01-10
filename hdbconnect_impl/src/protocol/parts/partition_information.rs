use crate::protocol::util;
#[cfg(feature = "async")]
use crate::protocol::util_async;
#[cfg(feature = "sync")]
use crate::protocol::util_sync;
#[cfg(feature = "sync")]
use byteorder::{LittleEndian, ReadBytesExt};

#[derive(Debug)]
#[allow(dead_code)]
pub struct PartitionInformation {
    partition_method: PartitionMethod,
    parameter_descriptor: Vec<ParameterDescriptor>,
    partitions: Vec<Partitions>,
}

#[derive(Debug, Clone, Copy)]
enum PartitionMethod {
    Invalid,
    RoundRobin,
    Hash,
}

impl PartitionMethod {
    pub fn from_i8(val: i8) -> std::io::Result<Self> {
        match val {
            0 => Ok(Self::Invalid),
            1 => Ok(Self::RoundRobin),
            2 => Ok(Self::Hash),
            _ => Err(util::io_error(format!(
                "PartitionMethod {} not implemented",
                val
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum ParameterFunction {
    Invalid,
    Year,
    Month,
}

impl ParameterFunction {
    pub fn from_i8(val: i8) -> std::io::Result<Self> {
        match val {
            0 => Ok(Self::Invalid),
            1 => Ok(Self::Year),
            2 => Ok(Self::Month),
            _ => Err(util::io_error(format!(
                "ParameterFunction {} not implemented",
                val
            ))),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub struct ParameterDescriptor {
    parameter_index: i32,
    parameter_function: ParameterFunction,
    attribute_type: i8,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub struct Partitions {
    val1: i32,
    val2: i32,
}

impl PartitionInformation {
    #[cfg(feature = "sync")]
    pub fn parse_sync(rdr: &mut dyn std::io::Read) -> std::io::Result<Self> {
        let partition_method = PartitionMethod::from_i8(rdr.read_i8()?)?; // I1
        util_sync::skip_bytes(7, rdr)?;
        let num_parameters = rdr.read_i32::<LittleEndian>()?;
        let num_partitions = rdr.read_i32::<LittleEndian>()?;
        let mut parameter_descriptor = vec![];
        for _ in 0..num_parameters {
            let desc = ParameterDescriptor {
                parameter_index: rdr.read_i32::<LittleEndian>()?,
                parameter_function: ParameterFunction::from_i8(rdr.read_i8()?)?,
                attribute_type: rdr.read_i8()?,
            };
            util_sync::skip_bytes(2, rdr)?;
            parameter_descriptor.push(desc);
        }

        let mut partitions = vec![];

        // Missing in documentation, but it is 8 byte per partition
        // https://help.sap.com/viewer/7e4aba181371442d9e4395e7ff71b777/2.0.03/en-US/a6b5b33a790245efa06c67a781f80d15.html#loioeed44c1df1fc4f139079f36031b42ef1
        for _ in 0..num_partitions {
            partitions.push({
                Partitions {
                    val1: rdr.read_i32::<LittleEndian>()?,
                    val2: rdr.read_i32::<LittleEndian>()?,
                }
            });
        }

        Ok(Self {
            partition_method,
            parameter_descriptor,
            partitions,
        })
    }

    #[cfg(feature = "async")]
    pub async fn parse_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
        rdr: &mut R,
    ) -> std::io::Result<Self> {
        let partition_method = PartitionMethod::from_i8(rdr.read_i8().await?)?; // I1
        util_async::skip_bytes(7, rdr).await?;
        let num_parameters = rdr.read_i32_le().await?;
        let num_partitions = rdr.read_i32_le().await?;
        let mut parameter_descriptor = vec![];
        for _ in 0..num_parameters {
            let desc = ParameterDescriptor {
                parameter_index: rdr.read_i32_le().await?,
                parameter_function: ParameterFunction::from_i8(rdr.read_i8().await?)?,
                attribute_type: rdr.read_i8().await?,
            };
            util_async::skip_bytes(2, rdr).await?;
            parameter_descriptor.push(desc);
        }

        let mut partitions = vec![];

        // Missing in documentation, but it is 8 byte per partition
        // https://help.sap.com/viewer/7e4aba181371442d9e4395e7ff71b777/2.0.03/en-US/a6b5b33a790245efa06c67a781f80d15.html#loioeed44c1df1fc4f139079f36031b42ef1
        for _ in 0..num_partitions {
            partitions.push({
                Partitions {
                    val1: rdr.read_i32_le().await?,
                    val2: rdr.read_i32_le().await?,
                }
            });
        }

        Ok(Self {
            partition_method,
            parameter_descriptor,
            partitions,
        })
    }
}