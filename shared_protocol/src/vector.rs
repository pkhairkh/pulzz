pub const MAX_EXACT_CHUNK_BYTES: usize = 32 * 1024;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_chunk_limit_is_stable() {
        assert_eq!(MAX_EXACT_CHUNK_BYTES, 32 * 1024);
    }
}
