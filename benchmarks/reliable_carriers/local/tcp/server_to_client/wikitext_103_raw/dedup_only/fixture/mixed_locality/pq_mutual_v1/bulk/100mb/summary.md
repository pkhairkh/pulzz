# Benchmark Summary

- case: `local:tcp:server_to_client:wikitext_103_raw:dedup_only:fixture:mixed_locality:pq_mutual_v1:bulk:native_rust:100mb`
- optimization: `dedup_only`
- client runtime: `native_rust`
- input corpus fingerprint: `8048a14e1542c3b87ee7ae46f5e0528fd17ac00379212dd43177b32f9b014ba6`
- input corpus files: `24209`
- input corpus chunks: `65538`
- target payload bytes: `100000000`
- original payload bytes: `99702120`
- actual payload bytes: `100000170`
- overshoot bytes: `170`
- protected wire bytes: `106718617`
- payload savings vs original: `-0.2989%`
- wire savings vs original: `-7.0375%`
- wire overhead vs encoded: `6.7184%`
- direct connectivity verified: `true`
- build profile: `release`
- sharded: `false`

## Server

- records: `373026`
- vector records: `298050`
- original payload bytes: `99702120`
- encoded payload throughput bytes/sec: `772337.28`
- wire throughput bytes/sec: `824226.27`
- peak rss bytes: `262586368`
- peak cpu percent: `99.40`

## Client

- records: `373026`
- vector records: `298050`
- original payload bytes: `99702120`
- encoded payload throughput bytes/sec: `636631.95`
- wire throughput bytes/sec: `679403.66`
- peak rss bytes: `231227392`
- peak cpu percent: `99.40`

## Corpus Utility

- measured events: `256`
- exact chunk top-1: `1.000000`
- exact chunk top-5: `1.000000`
- same file top-1: `1.000000`
- same file top-5: `1.000000`
- mean reciprocal rank: `1.000000`

## Source Cache

- lookups: `298050`
- hits: `233791`
- misses: `64259`
- embeddings skipped: `233791`
- cache read ns: `2700879617`
- cache write ns: `19436125556`

## Lane Profiles

- server: bl0_copy=`298050`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`
- client: bl0_copy=`298050`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`

## Source Families

- server: text=`298050`, json=`0`, binary=`0`, unknown=`0`
- client: text=`298050`, json=`0`, binary=`0`, unknown=`0`

## Residual Modes

- server: none=`298050`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
- client: none=`298050`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
