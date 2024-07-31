#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut data_block = self.data.clone();
        self.offsets.iter().for_each(|offset| {
            data_block.put_u16(*offset);
        });
        data_block.put_u16(self.offsets.len() as u16);
        return data_block.into();
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let number_of_elements = (&data[data.len() - SIZEOF_U16..]).get_u16() as usize;
        let data_end = data.len() - SIZEOF_U16 - number_of_elements * SIZEOF_U16;
        let offset_block = &data[data_end..data.len() - SIZEOF_U16];
        let offsets = offset_block.chunks(SIZEOF_U16).map(|mut elem| elem.get_u16()).collect();
        let data_block = data[0..data_end].to_vec();
        return Self { data: data_block, offsets };
    }
}
