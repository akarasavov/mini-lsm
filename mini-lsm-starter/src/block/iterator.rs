#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::key::{KeySlice, KeyVec};
use bytes::Buf;
use nom::Offset;
use std::cmp::Ordering;
use std::sync::Arc;

use super::{Block, SIZEOF_U16};

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    current_key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        debug_assert!(!block.data.is_empty(), "data block can't be empty");
        Self {
            block,
            current_key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iterator = BlockIterator::new(block);
        iterator.seek_to_first();
        return iterator;
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iterator = BlockIterator::new(block);
        iterator.seek_to_key(key);
        return iterator;
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        debug_assert!(!self.current_key.is_empty(), "invalid iterator");
        return self.current_key.as_key_slice();
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        debug_assert!(!self.current_key.is_empty(), "invalid iterator");
        return &self.block.data[self.value_range.0..self.value_range.1];
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        return !self.current_key.is_empty();
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to_key_with_index(0);
    }

    fn seek_to_key_with_offset(&mut self, start_offset: usize) {
        let key_len_end = start_offset + SIZEOF_U16;
        let key_len = (&self.block.data[start_offset..key_len_end]).get_u16() as usize;
        let key_end = key_len_end + key_len;
        self.current_key = KeyVec::from_vec(self.block.data[key_len_end..key_end].to_vec());
        let value_length_end = key_end + SIZEOF_U16;
        let value_length = (&self.block.data[key_end..value_length_end]).get_u16() as usize;
        self.value_range = (value_length_end, value_length_end + value_length);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.seek_to_key_with_index(self.idx + 1);
    }

    fn seek_to_key_with_index(&mut self, index: usize) {
        if index >= self.block.offsets.len() {
            self.current_key.clear();
            self.value_range = (0, 0);
            return;
        }
        self.idx = index;
        let offset = self.block.offsets[self.idx];
        self.seek_to_key_with_offset(offset as usize);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let mut low = 0;
        let mut high = self.block.offsets.len();
        while low < high {
            let mid = low + (high - low) / 2;
            self.seek_to_key_with_index(mid);
            assert!(self.is_valid());
            match self.key().cmp(&key) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid,
                std::cmp::Ordering::Equal => return,
            }
        }
        self.seek_to_key_with_index(low);
    }
}
