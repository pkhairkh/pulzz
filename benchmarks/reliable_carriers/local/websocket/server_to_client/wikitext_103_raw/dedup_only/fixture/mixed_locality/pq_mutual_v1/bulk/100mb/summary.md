# Benchmark Summary

- case: `local:websocket:server_to_client:wikitext_103_raw:dedup_only:fixture:mixed_locality:pq_mutual_v1:bulk:native_rust:100mb`
- optimization: `dedup_only`
- client runtime: `native_rust`
- input corpus fingerprint: `8048a14e1542c3b87ee7ae46f5e0528fd17ac00379212dd43177b32f9b014ba6`
- input corpus files: `24209`
- input corpus chunks: `65538`
- target payload bytes: `100000000`
- original payload bytes: `99702502`
- actual payload bytes: `100000344`
- overshoot bytes: `344`
- protected wire bytes: `106707667`
- payload savings vs original: `-0.2987%`
- wire savings vs original: `-7.0261%`
- wire overhead vs encoded: `6.7073%`
- direct connectivity verified: `true`
- build profile: `release`
- sharded: `false`

## Server

- records: `372408`
- vector records: `297842`
- original payload bytes: `99702502`
- encoded payload throughput bytes/sec: `849840.01`
- wire throughput bytes/sec: `906841.33`
- peak rss bytes: `263340032`
- peak cpu percent: `99.70`

## Client

- records: `372408`
- vector records: `297842`
- original payload bytes: `99702502`
- encoded payload throughput bytes/sec: `669503.05`
- wire throughput bytes/sec: `714408.63`
- peak rss bytes: `249380864`
- peak cpu percent: `99.30`

## Corpus Utility

- measured events: `256`
- exact chunk top-1: `1.000000`
- exact chunk top-5: `1.000000`
- same file top-1: `1.000000`
- same file top-5: `1.000000`
- mean reciprocal rank: `1.000000`

## Source Cache

- lookups: `297842`
- hits: `233520`
- misses: `64322`
- embeddings skipped: `233520`
- cache read ns: `2141307774`
- cache write ns: `18157913752`

## Lane Profiles

- server: bl0_copy=`297842`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`
- client: bl0_copy=`297842`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`

## Source Families

- server: text=`297842`, json=`0`, binary=`0`, unknown=`0`
- client: text=`297842`, json=`0`, binary=`0`, unknown=`0`

## Residual Modes

- server: none=`297842`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
- client: none=`297842`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
