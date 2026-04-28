# Exact-Byte Evaluation Summary

- security profile: `classic_ref1`
- preset: `default`
- description: `predictive-memory exact-state evaluation`
- events per workload: `160`

## high_locality

- payload events: `140`
- source kinds: text=`47`, json=`45`, binary=`48`
- codec modes: direct_exact=`97`, packed_exact=`0`, predicted_exact=`43`, control=`20`
- original bytes: `12855`
- encoded payload bytes: `18756`
- protected wire bytes: `27236`
- payload savings vs original: `-45.9043%`
- wire savings vs original: `-111.8709%`
- wire overhead vs encoded: `45.2122%`
- exact round-trip rate: `1.000000`
- avg encode ns: `594504.29`
- avg decode ns: `239157.42`


## Compression Pipeline (P0-P5)

- zstd_dict: `10`, zstd_raw: `0`, delta: `108`, template: `7`, passthrough: `15`
- original bytes: `12855`, compressed bytes: `12408`
- compression savings: `3.48%`
- dicts trained: `2`, templates registered: `1`
- avg compress ns: `58318.98`, avg decompress ns: `6344.01`
## mixed_locality

- payload events: `131`
- source kinds: text=`41`, json=`43`, binary=`47`
- codec modes: direct_exact=`106`, packed_exact=`0`, predicted_exact=`25`, control=`29`
- original bytes: `12311`
- encoded payload bytes: `15767`
- protected wire bytes: `24247`
- payload savings vs original: `-28.0725%`
- wire savings vs original: `-96.9539%`
- wire overhead vs encoded: `53.7832%`
- exact round-trip rate: `1.000000`
- avg encode ns: `463432.51`
- avg decode ns: `225027.22`


## Compression Pipeline (P0-P5)

- zstd_dict: `16`, zstd_raw: `0`, delta: `76`, template: `17`, passthrough: `22`
- original bytes: `12311`, compressed bytes: `11297`
- compression savings: `8.24%`
- dicts trained: `2`, templates registered: `1`
- avg compress ns: `59536.79`, avg decompress ns: `6659.47`
## low_locality

- payload events: `122`
- source kinds: text=`40`, json=`36`, binary=`46`
- codec modes: direct_exact=`111`, packed_exact=`0`, predicted_exact=`11`, control=`38`
- original bytes: `11478`
- encoded payload bytes: `13234`
- protected wire bytes: `21714`
- payload savings vs original: `-15.2988%`
- wire savings vs original: `-89.1793%`
- wire overhead vs encoded: `64.0774%`
- exact round-trip rate: `1.000000`
- avg encode ns: `322104.45`
- avg decode ns: `188546.01`


## Compression Pipeline (P0-P5)

- zstd_dict: `28`, zstd_raw: `0`, delta: `30`, template: `27`, passthrough: `37`
- original bytes: `11478`, compressed bytes: `9865`
- compression savings: `14.05%`
- dicts trained: `2`, templates registered: `1`
- avg compress ns: `35708.52`, avg decompress ns: `9408.34`
## Divergences

- evaluation presets now converge on the same exact-state lane
- evaluation baselines now compare original payload bytes, encoded payload bytes, and protected wire bytes; predictive-memory route metrics replaced semantic vector utility metrics
