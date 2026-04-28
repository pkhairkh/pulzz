# Benchmark Summary

- case: `local:quic_stream:server_to_client:wikitext_103_raw:dedup_only:fixture:mixed_locality:pq_simple_v1:burst_medium:native_rust:100mb`
- optimization: `dedup_only`
- client runtime: `native_rust`
- input corpus fingerprint: `8048a14e1542c3b87ee7ae46f5e0528fd17ac00379212dd43177b32f9b014ba6`
- input corpus files: `24209`
- input corpus chunks: `65538`
- target payload bytes: `100000000`
- original payload bytes: `99702193`
- actual payload bytes: `100000186`
- overshoot bytes: `186`
- protected wire bytes: `106773436`
- payload savings vs original: `-0.2989%`
- wire savings vs original: `-7.0924%`
- wire overhead vs encoded: `6.7732%`
- direct connectivity verified: `true`
- build profile: `release`
- sharded: `false`

## Server

- records: `372802`
- vector records: `297993`
- original payload bytes: `99702193`
- encoded payload throughput bytes/sec: `719572.99`
- wire throughput bytes/sec: `768311.37`
- peak rss bytes: `278659072`
- peak cpu percent: `102.40`

## Client

- records: `372802`
- vector records: `297993`
- original payload bytes: `99702193`
- encoded payload throughput bytes/sec: `719617.54`
- wire throughput bytes/sec: `768358.95`
- peak rss bytes: `278265856`
- peak cpu percent: `102.40`

## Corpus Utility

- measured events: `256`
- exact chunk top-1: `1.000000`
- exact chunk top-5: `1.000000`
- same file top-1: `1.000000`
- same file top-5: `1.000000`
- mean reciprocal rank: `1.000000`

## Source Cache

- lookups: `297993`
- hits: `233656`
- misses: `64337`
- embeddings skipped: `233656`
- cache read ns: `2503854210`
- cache write ns: `22532967463`

## Lane Profiles

- server: bl0_copy=`297993`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`
- client: bl0_copy=`297993`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`

## Source Families

- server: text=`297993`, json=`0`, binary=`0`, unknown=`0`
- client: text=`297993`, json=`0`, binary=`0`, unknown=`0`

## Residual Modes

- server: none=`297993`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
- client: none=`297993`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
