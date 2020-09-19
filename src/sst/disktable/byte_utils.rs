use std::convert::TryInto;

pub(crate) struct ByteUtils;
impl ByteUtils {
    pub fn as_usize(array: &[u8]) -> usize {
        u32::from_le_bytes(array.try_into().unwrap()) as usize
    }
    pub fn as_u64(array: &[u8]) -> u64 {
        u64::from_le_bytes(array.try_into().unwrap()) as u64
    }
    pub fn as_string(array: &[u8]) -> String {
        std::str::from_utf8(array).unwrap().to_string()
    }
    pub fn from_usize(n: usize) -> [u8; 4] {
        (n as u32).to_le_bytes()
    }
    pub fn from_u64(n: u64) -> [u8; 8] {
        (n as u64).to_le_bytes()
    }
}
