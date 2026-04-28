# Benchmark Summary

- case: `local:quic_stream:server_to_client:wikitext_103_raw:dedup_only:fixture:mixed_locality:pq_simple_v1:bulk:native_rust:100mb`
- optimization: `dedup_only`
- client runtime: `native_rust`
- input corpus fingerprint: `8048a14e1542c3b87ee7ae46f5e0528fd17ac00379212dd43177b32f9b014ba6`
- input corpus files: `24209`
- input corpus chunks: `65538`
- target payload bytes: `100000000`
- original payload bytes: `99702288`
- actual payload bytes: `100000377`
- overshoot bytes: `377`
- protected wire bytes: `106715710`
- payload savings vs original: `-0.2990%`
- wire savings vs original: `-7.0344%`
- wire overhead vs encoded: `6.7153%`
- direct connectivity verified: `true`
- build profile: `release`
- sharded: `false`

## Server

- records: `372853`
- vector records: `298089`
- original payload bytes: `99702288`
- encoded payload throughput bytes/sec: `729877.84`
- wire throughput bytes/sec: `778891.38`
- peak rss bytes: `284721152`
- peak cpu percent: `99.80`

## Client

- records: `372853`
- vector records: `298089`
- original payload bytes: `99702288`
- encoded payload throughput bytes/sec: `729853.33`
- wire throughput bytes/sec: `778865.23`
- peak rss bytes: `285196288`
- peak cpu percent: `103.60`

## Corpus Utility

- measured events: `256`
- exact chunk top-1: `1.000000`
- exact chunk top-5: `1.000000`
- same file top-1: `1.000000`
- same file top-5: `1.000000`
- mean reciprocal rank: `1.000000`

## Source Cache

- lookups: `298089`
- hits: `233766`
- misses: `64323`
- embeddings skipped: `233766`
- cache read ns: `2638887831`
- cache write ns: `21621067400`

## Lane Profiles

- server: bl0_copy=`298089`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`
- client: bl0_copy=`298089`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`

## Source Families

- server: text=`298089`, json=`0`, binary=`0`, unknown=`0`
- client: text=`298089`, json=`0`, binary=`0`, unknown=`0`

## Residual Modes

- server: none=`298089`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
- client: none=`298089`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
