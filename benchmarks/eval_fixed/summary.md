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
- encoded payload bytes: `18642`
- protected wire bytes: `27122`
- payload savings vs original: `-45.0175%`
- wire savings vs original: `-110.9841%`
- wire overhead vs encoded: `45.4887%`
- exact round-trip rate: `1.000000`
- avg encode ns: `505349.65`
- avg decode ns: `230208.04`

## mixed_locality

- payload events: `131`
- source kinds: text=`41`, json=`43`, binary=`47`
- codec modes: direct_exact=`106`, packed_exact=`0`, predicted_exact=`25`, control=`29`
- original bytes: `12311`
- encoded payload bytes: `15721`
- protected wire bytes: `24201`
- payload savings vs original: `-27.6988%`
- wire savings vs original: `-96.5803%`
- wire overhead vs encoded: `53.9406%`
- exact round-trip rate: `1.000000`
- avg encode ns: `408313.30`
- avg decode ns: `191757.47`

## low_locality

- payload events: `122`
- source kinds: text=`40`, json=`36`, binary=`46`
- codec modes: direct_exact=`111`, packed_exact=`0`, predicted_exact=`11`, control=`38`
- original bytes: `11478`
- encoded payload bytes: `13041`
- protected wire bytes: `21521`
- payload savings vs original: `-13.6174%`
- wire savings vs original: `-87.4978%`
- wire overhead vs encoded: `65.0257%`
- exact round-trip rate: `1.000000`
- avg encode ns: `309292.23`
- avg decode ns: `171410.86`

## Divergences

- evaluation presets now converge on the same exact-state lane
- evaluation baselines now compare original payload bytes, encoded payload bytes, and protected wire bytes; predictive-memory route metrics replaced semantic vector utility metrics
