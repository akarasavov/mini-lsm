#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use bytes::BufMut;
use crate::key::{KeySlice, KeyVec};

use super::{Block, SIZEOF_U16};

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self { offsets: vec![], data: vec![], block_size, first_key: KeyVec::new() }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if self.estimate_current_size() + key.len() + value.len() + 3 * SIZEOF_U16 > self.block_size && !self.first_key.is_empty() {
            return false;
        }
        self.offsets.push(self.data.len() as u16);
        self.data.put_u16(key.len() as u16);
        self.data.put_slice(key.raw_ref());
        self.data.put_u16(value.len() as u16);
        self.data.put_slice(value);
        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }
        return true;
    }

    fn estimate_current_size(&self) -> usize {
        return SIZEOF_U16 + self.offsets.len() * SIZEOF_U16 + self.data.len();
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        unimplemented!()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        return Block { data: self.data, offsets: self.offsets };
    }
}
