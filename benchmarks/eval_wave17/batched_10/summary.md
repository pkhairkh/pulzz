# Exact-Byte Evaluation Summary

- security profile: `classic_ref1`
- preset: `default`
- description: `predictive-memory exact-state evaluation`
- events per workload: `160`

## high_locality

- payload events: `140`
- source kinds: text=`47`, json=`45`, binary=`48`
- codec modes: direct_exact=`14`, packed_exact=`0`, predicted_exact=`0`, control=`0`
- original bytes: `12855`
- encoded payload bytes: `9447`
- protected wire bytes: `10189`
- payload savings vs original: `26.5111%`
- wire savings vs original: `20.7390%`
- wire overhead vs encoded: `7.8543%`
- P0 record zstd: `0` compressed, `14` not compressed, `0` bytes saved
- P0 pre-zstd payload: `12855`, post-zstd payload: `9447`, record zstd savings: `0.00%`
- exact round-trip rate: `1.000000`
- avg encode ns: `14540.47`
- avg decode ns: `2669.91`


## Compression Pipeline (P0-P5)

- zstd_dict: `0`, zstd_raw: `0`, delta: `0`, template: `0`, passthrough: `0`
- original bytes: `0`, compressed bytes: `0`
- compression savings: `0.00%`
- dicts trained: `0`, templates registered: `0`
- avg compress ns: `0.00`, avg decompress ns: `0.00`
## mixed_locality

- payload events: `131`
- source kinds: text=`41`, json=`43`, binary=`47`
- codec modes: direct_exact=`14`, packed_exact=`0`, predicted_exact=`0`, control=`0`
- original bytes: `12311`
- encoded payload bytes: `9115`
- protected wire bytes: `9857`
- payload savings vs original: `25.9605%`
- wire savings vs original: `19.9334%`
- wire overhead vs encoded: `8.1404%`
- P0 record zstd: `0` compressed, `14` not compressed, `0` bytes saved
- P0 pre-zstd payload: `12311`, post-zstd payload: `9115`, record zstd savings: `0.00%`
- exact round-trip rate: `1.000000`
- avg encode ns: `4185.28`
- avg decode ns: `2128.59`


## Compression Pipeline (P0-P5)

- zstd_dict: `0`, zstd_raw: `0`, delta: `0`, template: `0`, passthrough: `0`
- original bytes: `0`, compressed bytes: `0`
- compression savings: `0.00%`
- dicts trained: `0`, templates registered: `0`
- avg compress ns: `0.00`, avg decompress ns: `0.00`
## low_locality

- payload events: `122`
- source kinds: text=`40`, json=`36`, binary=`46`
- codec modes: direct_exact=`13`, packed_exact=`0`, predicted_exact=`0`, control=`0`
- original bytes: `11478`
- encoded payload bytes: `8797`
- protected wire bytes: `9486`
- payload savings vs original: `23.3577%`
- wire savings vs original: `17.3549%`
- wire overhead vs encoded: `7.8322%`
- P0 record zstd: `0` compressed, `13` not compressed, `0` bytes saved
- P0 pre-zstd payload: `11478`, post-zstd payload: `8797`, record zstd savings: `0.00%`
- exact round-trip rate: `1.000000`
- avg encode ns: `4528.30`
- avg decode ns: `2354.04`


## Compression Pipeline (P0-P5)

- zstd_dict: `0`, zstd_raw: `0`, delta: `0`, template: `0`, passthrough: `0`
- original bytes: `0`, compressed bytes: `0`
- compression savings: `0.00%`
- dicts trained: `0`, templates registered: `0`
- avg compress ns: `0.00`, avg decompress ns: `0.00`
## Divergences

- batched evaluation with batch_size=10 amortizes per-item overhead and exploits cross-item redundancy via whole-batch zstd compression
- batched evaluation achieves POSITIVE wire savings on compressible workloads
