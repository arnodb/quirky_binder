use std::io::Seek;

use bincode::DefaultOptions;
use serde::{de::Error, Deserialize, Serialize, Serializer};
use tempfile::{spooled_tempfile, SpooledTempFile};

const DEFAULT_MAX_SIZE_IN_MEMORY: usize = 4096;

const THIS_IS_THE_END: u64 = 0x_74_68_65_20_65_6e_64_21;

pub struct Buffer {
    file: SpooledTempFile,
    options: DefaultOptions,
}

impl Buffer {
    pub fn new() -> Self {
        Self::with_max_size_in_memory(DEFAULT_MAX_SIZE_IN_MEMORY)
    }

    pub fn with_max_size_in_memory(max_size_in_memory: usize) -> Self {
        let file = spooled_tempfile(max_size_in_memory);
        let options = DefaultOptions::new();
        Self { file, options }
    }

    pub fn push<Data: Serialize>(&mut self, data: Data) -> Result<(), bincode::Error> {
        let mut serializer = self.serializer();
        data.serialize(&mut serializer)
    }

    pub fn end_writing(mut self) -> Result<BufferReader, bincode::Error> {
        let mut serializer = self.serializer();
        serializer.serialize_u64(THIS_IS_THE_END)?;
        self.file.seek(std::io::SeekFrom::Start(0))?;
        Ok(BufferReader {
            deserializer: bincode::Deserializer::with_reader(self.file, self.options),
        })
    }

    fn serializer(&mut self) -> bincode::Serializer<&mut SpooledTempFile, &mut DefaultOptions> {
        bincode::Serializer::new(&mut self.file, &mut self.options)
    }
}

impl Default for Buffer {
    fn default() -> Self {
        Self::new()
    }
}

pub struct BufferReader {
    deserializer:
        bincode::Deserializer<bincode::de::read::IoReader<SpooledTempFile>, DefaultOptions>,
}

impl BufferReader {
    pub fn read<'de, Data: Deserialize<'de>>(&mut self) -> Result<Data, bincode::Error> {
        Data::deserialize(&mut self.deserializer)
    }

    pub fn end_reading(mut self) -> Result<(), bincode::Error> {
        let this_is_the_end = u64::deserialize(&mut self.deserializer)?;
        if this_is_the_end == THIS_IS_THE_END {
            Ok(())
        } else {
            Err(bincode::Error::custom("End tag did not match"))
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    #[test]
    fn test_memory() {
        let mut buffer = Buffer::with_max_size_in_memory(1024 * 1024);
        buffer.push("universe").unwrap();
        buffer.push(42).unwrap();
        let mut reader = buffer.end_writing().unwrap();
        assert_eq!(reader.read::<String>().unwrap(), "universe");
        assert_eq!(reader.read::<i32>().unwrap(), 42);
        reader.end_reading().unwrap();
    }

    #[test]
    fn test_rolled() {
        let mut buffer = Buffer::with_max_size_in_memory(1);
        buffer.push("universe").unwrap();
        buffer.push(42).unwrap();
        let mut reader = buffer.end_writing().unwrap();
        assert_eq!(reader.read::<String>().unwrap(), "universe");
        assert_eq!(reader.read::<i32>().unwrap(), 42);
        reader.end_reading().unwrap();
    }
}
