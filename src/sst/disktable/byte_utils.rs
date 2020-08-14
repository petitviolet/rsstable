pub struct ByteUtils;
impl ByteUtils {
    pub fn as_usize(array: [u8; 4]) -> usize {
        u32::from_le_bytes(array) as usize
    }
    pub fn as_string(array: &[u8]) -> String {
        std::str::from_utf8(array).unwrap().to_string()
    }
    pub fn from_usize(n: usize) -> [u8; 4] {
        (n as u32).to_le_bytes()
    }
}
