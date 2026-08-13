# Exact-Byte Evaluation Summary

- security profile: `classic_ref1`
- preset: `default`
- description: `predictive-memory exact-state evaluation`
- events per workload: `160`

## high_locality

- payload events: `140`
- source kinds: text=`47`, json=`45`, binary=`48`
- codec modes: direct_exact=`7`, packed_exact=`0`, predicted_exact=`0`, control=`0`
- original bytes: `12855`
- encoded payload bytes: `8511`
- protected wire bytes: `8882`
- payload savings vs original: `33.7923%`
- wire savings vs original: `30.9063%`
- wire overhead vs encoded: `4.3591%`
- P0 record zstd: `0` compressed, `7` not compressed, `0` bytes saved
- P0 pre-zstd payload: `12855`, post-zstd payload: `8511`, record zstd savings: `0.00%`
- exact round-trip rate: `1.000000`
- avg encode ns: `8372.95`
- avg decode ns: `3276.97`


## Compression Pipeline (P0-P5)

- zstd_dict: `0`, zstd_raw: `0`, delta: `0`, template: `0`, passthrough: `0`
- original bytes: `0`, compressed bytes: `0`
- compression savings: `0.00%`
- dicts trained: `0`, templates registered: `0`
- avg compress ns: `0.00`, avg decompress ns: `0.00`
## mixed_locality

- payload events: `131`
- source kinds: text=`41`, json=`43`, binary=`47`
- codec modes: direct_exact=`7`, packed_exact=`0`, predicted_exact=`0`, control=`0`
- original bytes: `12311`
- encoded payload bytes: `8336`
- protected wire bytes: `8707`
- payload savings vs original: `32.2882%`
- wire savings vs original: `29.2746%`
- wire overhead vs encoded: `4.4506%`
- P0 record zstd: `0` compressed, `7` not compressed, `0` bytes saved
- P0 pre-zstd payload: `12311`, post-zstd payload: `8336`, record zstd savings: `0.00%`
- exact round-trip rate: `1.000000`
- avg encode ns: `5149.42`
- avg decode ns: `2933.51`


## Compression Pipeline (P0-P5)

- zstd_dict: `0`, zstd_raw: `0`, delta: `0`, template: `0`, passthrough: `0`
- original bytes: `0`, compressed bytes: `0`
- compression savings: `0.00%`
- dicts trained: `0`, templates registered: `0`
- avg compress ns: `0.00`, avg decompress ns: `0.00`
## low_locality

- payload events: `122`
- source kinds: text=`40`, json=`36`, binary=`46`
- codec modes: direct_exact=`7`, packed_exact=`0`, predicted_exact=`0`, control=`0`
- original bytes: `11478`
- encoded payload bytes: `8048`
- protected wire bytes: `8419`
- payload savings vs original: `29.8833%`
- wire savings vs original: `26.6510%`
- wire overhead vs encoded: `4.6098%`
- P0 record zstd: `0` compressed, `7` not compressed, `0` bytes saved
- P0 pre-zstd payload: `11478`, post-zstd payload: `8048`, record zstd savings: `0.00%`
- exact round-trip rate: `1.000000`
- avg encode ns: `5500.99`
- avg decode ns: `3284.39`


## Compression Pipeline (P0-P5)

- zstd_dict: `0`, zstd_raw: `0`, delta: `0`, template: `0`, passthrough: `0`
- original bytes: `0`, compressed bytes: `0`
- compression savings: `0.00%`
- dicts trained: `0`, templates registered: `0`
- avg compress ns: `0.00`, avg decompress ns: `0.00`
## Divergences

- batched evaluation with batch_size=20 amortizes per-item overhead and exploits cross-item redundancy via whole-batch zstd compression
- batched evaluation achieves POSITIVE wire savings on compressible workloads
