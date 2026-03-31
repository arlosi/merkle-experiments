use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
};

use hex::FromHexError;
use sha2::{Digest as _, Sha256};

#[derive(Debug, Clone)]
pub enum Node {
    Index(IndexNode),
    Leaf(LeafNode),
}

impl Node {
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            Node::Index(index_node) => index_node.serialize(),
            Node::Leaf(leaf_node) => leaf_node.serialize(),
        }
    }

    pub fn deserialize(data: Vec<u8>) -> Self {
        let first = data[0];
        match first {
            IndexNode::ID => Node::Index(IndexNode::deserialize(data)),
            LeafNode::ID => Node::Leaf(LeafNode::deserialize(data)),
            _ => panic!("TODO"),
        }
    }

    pub fn as_leaf(&self) -> Option<&LeafNode> {
        match self {
            Node::Index(_) => None,
            Node::Leaf(leaf_node) => Some(leaf_node),
        }
    }

    pub fn as_index(&self) -> Option<&IndexNode> {
        match self {
            Node::Index(index_node) => Some(index_node),
            Node::Leaf(_) => None,
        }
    }
}

#[derive(Clone)]
pub struct IndexNode(Vec<ContentHash>);

impl IndexNode {
    const ID: u8 = 0;

    pub fn new(nibble_width: u8) -> Self {
        assert!(nibble_width > 0);
        Self(vec![
            ContentHash([0; ContentHash::SIZE]);
            ContentHash::count(nibble_width)
        ])
    }

    pub fn deserialize(data: Vec<u8>) -> Self {
        let chunks: Vec<_> = data[1..]
            .chunks_exact(ContentHash::SIZE)
            .map(|c| ContentHash(c.try_into().unwrap()))
            .collect();
        IndexNode(chunks)
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.0.len() + 1);
        out.push(Self::ID);
        for i in &self.0 {
            out.extend_from_slice(&i.0);
        }
        out
    }

    pub fn len_bits(&self) -> u8 {
        let bredth = self.0.len().ilog2() as u8;
        debug_assert!(1 << (bredth as usize) == self.0.len());
        bredth
    }

    pub fn get(&self, key: SplitNameHash) -> Option<&ContentHash> {
        let v = self.0.get(key.0 as usize).unwrap();
        if v.0 == [0; ContentHash::SIZE] {
            None
        } else {
            Some(v)
        }
    }

    pub fn insert(&mut self, key: SplitNameHash, value: ContentHash) {
        self.0[key.0 as usize] = value;
    }

    pub fn iter(&self) -> impl Iterator<Item = &ContentHash> {
        self.0.iter().filter(|node| node.0 != [0u8; 32])
    }
}

impl Debug for IndexNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut m = f.debug_map();
        for (index, hash) in self.0.iter().enumerate() {
            if hash.0 != [0u8; 32] {
                m.entry(&SplitNameHash(index as u32, 16), hash);
            }
        }
        m.finish()
    }
}

#[derive(Debug, Clone)]
pub struct LeafNode(BTreeMap<String, ContentHash>);

impl LeafNode {
    const ID: u8 = 1;

    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    pub fn get(&self, key: &str) -> Option<&ContentHash> {
        self.0.get(key)
    }

    pub fn insert(&mut self, key: String, value: ContentHash) {
        self.0.insert(key, value);
    }

    pub fn deserialize(data: Vec<u8>) -> Self {
        let mut out = BTreeMap::new();
        let mut unparsed = &data[1..];
        while let Some(i) = unparsed.iter().position(|&b| b == 0) {
            let key = str::from_utf8(&unparsed[..i]).unwrap();
            unparsed = &unparsed[i + 1..];
            let value = &unparsed[..ContentHash::SIZE];
            unparsed = &unparsed[ContentHash::SIZE..];
            out.insert(key.to_owned(), ContentHash(value.try_into().unwrap()));
        }
        LeafNode(out)
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.0.len() * 64);
        out.push(Self::ID);
        for (k, v) in &self.0 {
            out.extend_from_slice(k.as_bytes());
            out.push(0);
            out.extend_from_slice(&v.0);
        }
        out
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &ContentHash)> {
        self.0.iter()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ContentHash([u8; ContentHash::SIZE]);

impl Debug for ContentHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl Display for ContentHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut output = [0u8; ContentHash::SIZE * 2];
        hex::encode_to_slice(&self.0, &mut output).unwrap();
        f.write_str(std::str::from_utf8(&output).unwrap())
    }
}

impl From<[u8; ContentHash::SIZE]> for ContentHash {
    fn from(value: [u8; 32]) -> Self {
        ContentHash(value)
    }
}

impl TryFrom<&str> for ContentHash {
    type Error = FromHexError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let mut data = [0u8; ContentHash::SIZE];
        hex::decode_to_slice(value.as_bytes(), &mut data)?;
        Ok(ContentHash(data))
    }
}

impl ContentHash {
    const SIZE: usize = 32;

    pub fn count(nibble_width: u8) -> usize {
        1 << nibble_width
    }
}


#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct SplitNameHash(u32, u8);

impl SplitNameHash {
    const MAX: u8 = 32;

    pub fn new(value: &str, len_in_bits: u8) -> SplitNameHash {
        assert!(len_in_bits <= Self::MAX);
        let bytes = Sha256::digest(value.as_bytes());
        SplitNameHash(u32::from_le_bytes(bytes[..4].try_into().unwrap()), 32).split(len_in_bits).1
    }

    /// Split the key, indexed from the right (0 is the rightmost).
    pub fn split(self, at: u8) -> (SplitNameHash, SplitNameHash) {
        debug_assert!(at <= self.1);
        let x = self.0;
        let left = x.checked_shr(at as u32).unwrap_or(0);
        let right = x & ((1u64 << at) - 1) as u32;
        (SplitNameHash(left, self.1 - at), SplitNameHash(right, at))
    }

    pub fn is_empty(self) -> bool {
        self.1 == 0
    }
}

impl Debug for SplitNameHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:0w$b}({})", self.0, self.1, w = self.1 as usize,)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic() {
        let (l, r) = SplitNameHash(0b1010_1100_1111_0001_0011_0101_1001_0110, 32).split(16);
        assert_eq!(l.0, 0b1010_1100_1111_0001);
        assert_eq!(r.0, 0b0011_0101_1001_0110);

        let (l, r) = SplitNameHash(0b1010_1100_1111_0001_0011_0101_1001_0110, 32).split(17);
        assert_eq!(l.0, 0b1010_1100_1111_000);
        assert_eq!(r.0, 0b10011_0101_1001_0110);
    }

    #[test]
    fn limits() {
        let x = 0b1010_1100_1111_0001_0011_0101_1001_0110;
        let (l, r) = SplitNameHash(x, 32).split(0);
        assert_eq!(l.0, x);
        assert_eq!(r.0, 0);
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     fn mk(data: u32) -> NameHash {
//         NameHash(data.to_be_bytes().try_into().unwrap())
//     }

//     #[test]
//     fn full_range_identity() {
//         let d = 0b1010_1100_1111_0001_0011_0101_1001_0110;
//         let k = mk(d);
//         let v = k.subkey(0, 31);
//         assert_eq!(v.0, d >> 1);
//     }

//     #[test]
//     fn single_bit_basic() {
//         let k = mk(0b1000_0000_0000_0000_0000_0000_0000_0000);
//         assert_eq!(k.subkey(0, 1).0, 1);

//         let k = mk(0b0100_0000_0000_0000_0000_0000_0000_0000);
//         assert_eq!(k.subkey(1, 1).0, 1);
//     }

//     #[test]
//     fn simple_4bit_window() {
//         // 0b1011_0000_0000_0000...
//         let k = mk(0b1011_0000_0000_0000_0000_0000_0000_0000);

//         // first 4 bits = 1011
//         assert_eq!(k.subkey(0, 4).0, 0b1011);
//     }

//     #[test]
//     fn middle_slice() {
//         let k = mk(0b1111_0000_1010_1100_0000_0000_0000_0000);

//         // bits 4..8 from MSB side
//         let v = k.subkey(4, 4);
//         assert_eq!(v.0, 0b0000);
//     }

//     #[test]
//     fn cross_byte_boundary() {
//         let k = mk(0b0000_1111_1111_0000_0000_0000_0000_0000);

//         // bits spanning boundary
//         let v = k.subkey(1, 8);
//         assert_eq!(v.0, 0b1111_0000);
//     }

//     #[test]
//     fn cross_byte_boundary_large() {
//         let k = mk(0b0000_0000_0000_1101_0111_1011_1000_0000);

//         // bits spanning boundary
//         let v = k.subkey(1, 13);
//         assert_eq!(v.0, 0b101_0111_1011_10);
//     }

//     #[test]
//     fn max_allowed_slice_31_bits() {
//         let k = mk(0xFFFF_FFFF);
//         let v = k.subkey(0, 31);
//         assert_eq!(v.0, 0x7FFF_FFFF); // sanity shape check
//     }

//     #[test]
//     fn deterministic_repeatable() {
//         let k1 = mk(0x1234_5678);

//         assert_eq!(k1.subkey(1, 16).0, 0x5678);
//     }
// }
