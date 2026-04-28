# Benchmark Summary

- case: `local:websocket:server_to_client:wikitext_103_raw:dedup_only:fixture:mixed_locality:pq_simple_v1:bulk:native_rust:100mb`
- optimization: `dedup_only`
- client runtime: `native_rust`
- input corpus fingerprint: `8048a14e1542c3b87ee7ae46f5e0528fd17ac00379212dd43177b32f9b014ba6`
- input corpus files: `24209`
- input corpus chunks: `65538`
- target payload bytes: `100000000`
- original payload bytes: `99702370`
- actual payload bytes: `100000217`
- overshoot bytes: `217`
- protected wire bytes: `106712688`
- payload savings vs original: `-0.2987%`
- wire savings vs original: `-7.0312%`
- wire overhead vs encoded: `6.7125%`
- direct connectivity verified: `true`
- build profile: `release`
- sharded: `false`

## Server

- records: `372694`
- vector records: `297847`
- original payload bytes: `99702370`
- encoded payload throughput bytes/sec: `526834.79`
- wire throughput bytes/sec: `562198.35`
- peak rss bytes: `283213824`
- peak cpu percent: `99.60`

## Client

- records: `372694`
- vector records: `297847`
- original payload bytes: `99702370`
- encoded payload throughput bytes/sec: `449634.07`
- wire throughput bytes/sec: `479815.56`
- peak rss bytes: `273350656`
- peak cpu percent: `99.60`

## Corpus Utility

- measured events: `256`
- exact chunk top-1: `1.000000`
- exact chunk top-5: `1.000000`
- same file top-1: `1.000000`
- same file top-5: `1.000000`
- mean reciprocal rank: `1.000000`

## Source Cache

- lookups: `297847`
- hits: `233539`
- misses: `64308`
- embeddings skipped: `233539`
- cache read ns: `5294445957`
- cache write ns: `33220076124`

## Lane Profiles

- server: bl0_copy=`297847`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`
- client: bl0_copy=`297847`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`

## Source Families

- server: text=`297847`, json=`0`, binary=`0`, unknown=`0`
- client: text=`297847`, json=`0`, binary=`0`, unknown=`0`

## Residual Modes

- server: none=`297847`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
- client: none=`297847`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
