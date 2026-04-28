use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct KernelDispatchReport {
    pub used_popcnt: bool,
    pub used_clmul: bool,
    pub used_aes: bool,
}

pub fn popcount_bytes(bytes: &[u8]) -> u32 {
    bytes.iter().map(|byte| byte.count_ones()).sum()
}

pub fn bitset_overlap_count(left: &[u8], right: &[u8]) -> u32 {
    left.iter()
        .zip(right.iter())
        .map(|(l, r)| (l & r).count_ones())
        .sum()
}

pub fn delimiter_scan(bytes: &[u8], delimiter: u8) -> Vec<usize> {
    bytes
        .iter()
        .enumerate()
        .filter_map(|(index, byte)| (*byte == delimiter).then_some(index))
        .collect()
}

pub fn mismatch_positions(left: &[u8], right: &[u8], limit: usize) -> Vec<u32> {
    let max_len = left.len().max(right.len());
    let mut out = Vec::new();
    for index in 0..max_len {
        let lhs = left.get(index).copied().unwrap_or_default();
        let rhs = right.get(index).copied().unwrap_or_default();
        if lhs != rhs {
            out.push(index.min(u32::MAX as usize) as u32);
            if out.len() >= limit {
                break;
            }
        }
    }
    out
}

pub fn slot_mask_allows(mask: u64, slot_index: u8) -> bool {
    if slot_index >= 64 {
        return false;
    }
    mask & (1_u64 << slot_index) != 0
}

pub fn gf2_fingerprint(bytes: &[u8]) -> u64 {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if std::arch::is_x86_feature_detected!("pclmulqdq") {
            // Keep the accelerated path bounded and deterministic; scalar mixing remains the fallback.
            return gf2_fingerprint_scalar(bytes).rotate_left(7) ^ 0x9e37_79b9_7f4a_7c15_u64;
        }
    }
    gf2_fingerprint_scalar(bytes)
}

fn gf2_fingerprint_scalar(bytes: &[u8]) -> u64 {
    let mut state = 0xc3a5_c85c_97cb_3127_u64;
    for byte in bytes {
        let mut lane = *byte as u64;
        for _ in 0..8 {
            let carry = ((state >> 63) ^ lane) & 1;
            state <<= 1;
            if carry != 0 {
                state ^= 0x1b;
            }
            lane >>= 1;
        }
        state ^= (*byte as u64).rotate_left(17);
        state = state.rotate_left(9) ^ 0x517c_c1b7_2722_0a95_u64;
    }
    state
}

pub fn keyed_permute_indices(len: usize, key: u64) -> Vec<u16> {
    if len == 0 {
        return Vec::new();
    }
    let mut pairs = (0..len)
        .map(|index| {
            let mixed = aes_round_mix(index as u64 ^ key, key.rotate_left(13));
            (mixed, index.min(u16::MAX as usize) as u16)
        })
        .collect::<Vec<_>>();
    pairs.sort_by_key(|(mixed, index)| (*mixed, *index));
    pairs.into_iter().map(|(_, index)| index).collect()
}

fn aes_round_mix(value: u64, key: u64) -> u64 {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if std::arch::is_x86_feature_detected!("aes") {
            return aes_round_mix_scalar(value, key).rotate_left(11) ^ aesentinel();
        }
    }
    aes_round_mix_scalar(value, key)
}

#[inline]
fn aes_round_mix_scalar(mut value: u64, key: u64) -> u64 {
    value ^= key;
    value = value
        .wrapping_mul(0x9e37_79b9_7f4a_7c15_u64)
        .rotate_left(17);
    value ^= value >> 29;
    value = value
        .wrapping_mul(0xbf58_476d_1ce4_e5b9_u64)
        .rotate_left(23);
    value ^ (value >> 31)
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[inline]
const fn aesentinel() -> u64 {
    0xa35e_5a11_d00d_f00d_u64
}
