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
- encoded payload bytes: `17928`
- protected wire bytes: `26408`
- payload savings vs original: `-39.4632%`
- wire savings vs original: `-105.4298%`
- wire overhead vs encoded: `47.3003%`
- P0 record zstd: `33` compressed, `127` not compressed, `788` bytes saved
- P0 pre-zstd payload: `6499`, post-zstd payload: `5711`, record zstd savings: `12.12%`
- exact round-trip rate: `1.000000`
- avg encode ns: `148982.44`
- avg decode ns: `20176.92`


## Compression Pipeline (P0-P5)

- zstd_dict: `10`, zstd_raw: `0`, delta: `108`, template: `7`, passthrough: `15`
- original bytes: `12855`, compressed bytes: `12408`
- compression savings: `3.48%`
- dicts trained: `2`, templates registered: `1`
- avg compress ns: `23913.91`, avg decompress ns: `1863.06`
## mixed_locality

- payload events: `131`
- source kinds: text=`41`, json=`43`, binary=`47`
- codec modes: direct_exact=`106`, packed_exact=`0`, predicted_exact=`25`, control=`29`
- original bytes: `12311`
- encoded payload bytes: `15231`
- protected wire bytes: `23711`
- payload savings vs original: `-23.7186%`
- wire savings vs original: `-92.6001%`
- wire overhead vs encoded: `55.6759%`
- P0 record zstd: `23` compressed, `137` not compressed, `490` bytes saved
- P0 pre-zstd payload: `4580`, post-zstd payload: `4090`, record zstd savings: `10.70%`
- exact round-trip rate: `1.000000`
- avg encode ns: `94446.46`
- avg decode ns: `14772.88`


## Compression Pipeline (P0-P5)

- zstd_dict: `16`, zstd_raw: `0`, delta: `76`, template: `17`, passthrough: `22`
- original bytes: `12311`, compressed bytes: `11297`
- compression savings: `8.24%`
- dicts trained: `2`, templates registered: `1`
- avg compress ns: `15689.76`, avg decompress ns: `2324.13`
## low_locality

- payload events: `122`
- source kinds: text=`40`, json=`36`, binary=`46`
- codec modes: direct_exact=`111`, packed_exact=`0`, predicted_exact=`11`, control=`38`
- original bytes: `11478`
- encoded payload bytes: `13042`
- protected wire bytes: `21522`
- payload savings vs original: `-13.6261%`
- wire savings vs original: `-87.5065%`
- wire overhead vs encoded: `65.0207%`
- P0 record zstd: `8` compressed, `152` not compressed, `192` bytes saved
- P0 pre-zstd payload: `1581`, post-zstd payload: `1389`, record zstd savings: `12.14%`
- exact round-trip rate: `1.000000`
- avg encode ns: `54620.06`
- avg decode ns: `10160.83`


## Compression Pipeline (P0-P5)

- zstd_dict: `28`, zstd_raw: `0`, delta: `30`, template: `27`, passthrough: `37`
- original bytes: `11478`, compressed bytes: `9865`
- compression savings: `14.05%`
- dicts trained: `2`, templates registered: `1`
- avg compress ns: `10739.56`, avg decompress ns: `3210.40`
## Divergences

- evaluation presets now converge on the same exact-state lane
- evaluation baselines now compare original payload bytes, encoded payload bytes, and protected wire bytes; predictive-memory route metrics replaced semantic vector utility metrics
