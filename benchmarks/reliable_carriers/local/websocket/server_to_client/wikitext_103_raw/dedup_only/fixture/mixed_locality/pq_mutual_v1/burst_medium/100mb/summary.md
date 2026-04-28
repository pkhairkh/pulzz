# Benchmark Summary

- case: `local:websocket:server_to_client:wikitext_103_raw:dedup_only:fixture:mixed_locality:pq_mutual_v1:burst_medium:native_rust:100mb`
- optimization: `dedup_only`
- client runtime: `native_rust`
- input corpus fingerprint: `8048a14e1542c3b87ee7ae46f5e0528fd17ac00379212dd43177b32f9b014ba6`
- input corpus files: `24209`
- input corpus chunks: `65538`
- target payload bytes: `100000000`
- original payload bytes: `99702463`
- actual payload bytes: `100000236`
- overshoot bytes: `236`
- protected wire bytes: `106760544`
- payload savings vs original: `-0.2987%`
- wire savings vs original: `-7.0791%`
- wire overhead vs encoded: `6.7603%`
- direct connectivity verified: `true`
- build profile: `release`
- sharded: `false`

## Server

- records: `372083`
- vector records: `297773`
- original payload bytes: `99702463`
- encoded payload throughput bytes/sec: `740861.59`
- wire throughput bytes/sec: `790946.00`
- peak rss bytes: `267403264`
- peak cpu percent: `98.10`

## Client

- records: `372083`
- vector records: `297773`
- original payload bytes: `99702463`
- encoded payload throughput bytes/sec: `741438.09`
- wire throughput bytes/sec: `791561.47`
- peak rss bytes: `267403264`
- peak cpu percent: `98.10`

## Corpus Utility

- measured events: `256`
- exact chunk top-1: `1.000000`
- exact chunk top-5: `1.000000`
- same file top-1: `1.000000`
- same file top-5: `1.000000`
- mean reciprocal rank: `1.000000`

## Source Cache

- lookups: `297773`
- hits: `233521`
- misses: `64252`
- embeddings skipped: `233521`
- cache read ns: `3885760100`
- cache write ns: `24262265631`

## Lane Profiles

- server: bl0_copy=`297773`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`
- client: bl0_copy=`297773`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`

## Source Families

- server: text=`297773`, json=`0`, binary=`0`, unknown=`0`
- client: text=`297773`, json=`0`, binary=`0`, unknown=`0`

## Residual Modes

- server: none=`297773`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
- client: none=`297773`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
