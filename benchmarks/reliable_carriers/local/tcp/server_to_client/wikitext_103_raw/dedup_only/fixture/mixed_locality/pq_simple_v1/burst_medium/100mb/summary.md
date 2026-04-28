# Benchmark Summary

- case: `local:tcp:server_to_client:wikitext_103_raw:dedup_only:fixture:mixed_locality:pq_simple_v1:burst_medium:native_rust:100mb`
- optimization: `dedup_only`
- client runtime: `native_rust`
- input corpus fingerprint: `8048a14e1542c3b87ee7ae46f5e0528fd17ac00379212dd43177b32f9b014ba6`
- input corpus files: `24209`
- input corpus chunks: `65538`
- target payload bytes: `100000000`
- original payload bytes: `99702244`
- actual payload bytes: `100000152`
- overshoot bytes: `152`
- protected wire bytes: `106763533`
- payload savings vs original: `-0.2988%`
- wire savings vs original: `-7.0824%`
- wire overhead vs encoded: `6.7634%`
- direct connectivity verified: `true`
- build profile: `release`
- sharded: `false`

## Server

- records: `372256`
- vector records: `297908`
- original payload bytes: `99702244`
- encoded payload throughput bytes/sec: `766004.74`
- wire throughput bytes/sec: `817812.48`
- peak rss bytes: `278626304`
- peak cpu percent: `101.30`

## Client

- records: `372256`
- vector records: `297908`
- original payload bytes: `99702244`
- encoded payload throughput bytes/sec: `766589.15`
- wire throughput bytes/sec: `818436.41`
- peak rss bytes: `278806528`
- peak cpu percent: `101.30`

## Corpus Utility

- measured events: `256`
- exact chunk top-1: `1.000000`
- exact chunk top-5: `1.000000`
- same file top-1: `1.000000`
- same file top-5: `1.000000`
- mean reciprocal rank: `1.000000`

## Source Cache

- lookups: `297908`
- hits: `233591`
- misses: `64317`
- embeddings skipped: `233591`
- cache read ns: `4312179627`
- cache write ns: `24083669211`

## Lane Profiles

- server: bl0_copy=`297908`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`
- client: bl0_copy=`297908`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`

## Source Families

- server: text=`297908`, json=`0`, binary=`0`, unknown=`0`
- client: text=`297908`, json=`0`, binary=`0`, unknown=`0`

## Residual Modes

- server: none=`297908`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
- client: none=`297908`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
