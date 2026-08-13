# Exact-Byte Evaluation Summary

- security profile: `classic_ref1`
- preset: `default`
- description: `predictive-memory exact-state evaluation`
- events per workload: `160`

## high_locality

- payload events: `140`
- source kinds: text=`47`, json=`45`, binary=`48`
- codec modes: direct_exact=`3`, packed_exact=`0`, predicted_exact=`0`, control=`0`
- original bytes: `12855`
- encoded payload bytes: `7980`
- protected wire bytes: `8139`
- payload savings vs original: `37.9230%`
- wire savings vs original: `36.6861%`
- wire overhead vs encoded: `1.9925%`
- P0 record zstd: `0` compressed, `3` not compressed, `0` bytes saved
- P0 pre-zstd payload: `12855`, post-zstd payload: `7980`, record zstd savings: `0.00%`
- exact round-trip rate: `1.000000`
- avg encode ns: `8990.26`
- avg decode ns: `3042.05`


## Compression Pipeline (P0-P5)

- zstd_dict: `0`, zstd_raw: `0`, delta: `0`, template: `0`, passthrough: `0`
- original bytes: `0`, compressed bytes: `0`
- compression savings: `0.00%`
- dicts trained: `0`, templates registered: `0`
- avg compress ns: `0.00`, avg decompress ns: `0.00`
## mixed_locality

- payload events: `131`
- source kinds: text=`41`, json=`43`, binary=`47`
- codec modes: direct_exact=`3`, packed_exact=`0`, predicted_exact=`0`, control=`0`
- original bytes: `12311`
- encoded payload bytes: `7775`
- protected wire bytes: `7934`
- payload savings vs original: `36.8451%`
- wire savings vs original: `35.5536%`
- wire overhead vs encoded: `2.0450%`
- P0 record zstd: `0` compressed, `3` not compressed, `0` bytes saved
- P0 pre-zstd payload: `12311`, post-zstd payload: `7775`, record zstd savings: `0.00%`
- exact round-trip rate: `1.000000`
- avg encode ns: `3176.44`
- avg decode ns: `2798.38`


## Compression Pipeline (P0-P5)

- zstd_dict: `0`, zstd_raw: `0`, delta: `0`, template: `0`, passthrough: `0`
- original bytes: `0`, compressed bytes: `0`
- compression savings: `0.00%`
- dicts trained: `0`, templates registered: `0`
- avg compress ns: `0.00`, avg decompress ns: `0.00`
## low_locality

- payload events: `122`
- source kinds: text=`40`, json=`36`, binary=`46`
- codec modes: direct_exact=`3`, packed_exact=`0`, predicted_exact=`0`, control=`0`
- original bytes: `11478`
- encoded payload bytes: `7592`
- protected wire bytes: `7751`
- payload savings vs original: `33.8561%`
- wire savings vs original: `32.4708%`
- wire overhead vs encoded: `2.0943%`
- P0 record zstd: `0` compressed, `3` not compressed, `0` bytes saved
- P0 pre-zstd payload: `11478`, post-zstd payload: `7592`, record zstd savings: `0.00%`
- exact round-trip rate: `1.000000`
- avg encode ns: `3090.70`
- avg decode ns: `2568.45`


## Compression Pipeline (P0-P5)

- zstd_dict: `0`, zstd_raw: `0`, delta: `0`, template: `0`, passthrough: `0`
- original bytes: `0`, compressed bytes: `0`
- compression savings: `0.00%`
- dicts trained: `0`, templates registered: `0`
- avg compress ns: `0.00`, avg decompress ns: `0.00`
## Divergences

- batched evaluation with batch_size=50 amortizes per-item overhead and exploits cross-item redundancy via whole-batch zstd compression
- batched evaluation achieves POSITIVE wire savings on compressible workloads
