# Benchmark Summary

- case: `local:tcp:server_to_client:wikitext_103_raw:dedup_only:fixture:mixed_locality:pq_mutual_v1:burst_medium:native_rust:100mb`
- optimization: `dedup_only`
- client runtime: `native_rust`
- input corpus fingerprint: `8048a14e1542c3b87ee7ae46f5e0528fd17ac00379212dd43177b32f9b014ba6`
- input corpus files: `24209`
- input corpus chunks: `65538`
- target payload bytes: `100000000`
- original payload bytes: `99701778`
- actual payload bytes: `100000085`
- overshoot bytes: `85`
- protected wire bytes: `106779414`
- payload savings vs original: `-0.2992%`
- wire savings vs original: `-7.0988%`
- wire overhead vs encoded: `6.7793%`
- direct connectivity verified: `true`
- build profile: `release`
- sharded: `false`

## Server

- records: `373142`
- vector records: `298307`
- original payload bytes: `99701778`
- encoded payload throughput bytes/sec: `711061.20`
- wire throughput bytes/sec: `759266.33`
- peak rss bytes: `248987648`
- peak cpu percent: `99.90`

## Client

- records: `373142`
- vector records: `298307`
- original payload bytes: `99701778`
- encoded payload throughput bytes/sec: `711552.09`
- wire throughput bytes/sec: `759790.51`
- peak rss bytes: `249380864`
- peak cpu percent: `100.70`

## Corpus Utility

- measured events: `256`
- exact chunk top-1: `1.000000`
- exact chunk top-5: `1.000000`
- same file top-1: `1.000000`
- same file top-5: `1.000000`
- mean reciprocal rank: `1.000000`

## Source Cache

- lookups: `298307`
- hits: `234043`
- misses: `64264`
- embeddings skipped: `234043`
- cache read ns: `3450060387`
- cache write ns: `23969077616`

## Lane Profiles

- server: bl0_copy=`298307`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`
- client: bl0_copy=`298307`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`

## Source Families

- server: text=`298307`, json=`0`, binary=`0`, unknown=`0`
- client: text=`298307`, json=`0`, binary=`0`, unknown=`0`

## Residual Modes

- server: none=`298307`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
- client: none=`298307`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
