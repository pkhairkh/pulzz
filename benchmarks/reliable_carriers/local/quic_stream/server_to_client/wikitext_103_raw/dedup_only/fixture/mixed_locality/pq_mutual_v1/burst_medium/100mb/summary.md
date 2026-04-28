# Benchmark Summary

- case: `local:quic_stream:server_to_client:wikitext_103_raw:dedup_only:fixture:mixed_locality:pq_mutual_v1:burst_medium:native_rust:100mb`
- optimization: `dedup_only`
- client runtime: `native_rust`
- input corpus fingerprint: `8048a14e1542c3b87ee7ae46f5e0528fd17ac00379212dd43177b32f9b014ba6`
- input corpus files: `24209`
- input corpus chunks: `65538`
- target payload bytes: `100000000`
- original payload bytes: `99702035`
- actual payload bytes: `100000019`
- overshoot bytes: `19`
- protected wire bytes: `106776774`
- payload savings vs original: `-0.2989%`
- wire savings vs original: `-7.0959%`
- wire overhead vs encoded: `6.7768%`
- direct connectivity verified: `true`
- build profile: `release`
- sharded: `false`

## Server

- records: `372999`
- vector records: `297984`
- original payload bytes: `99702035`
- encoded payload throughput bytes/sec: `769417.09`
- wire throughput bytes/sec: `821558.59`
- peak rss bytes: `290652160`
- peak cpu percent: `102.50`

## Client

- records: `372999`
- vector records: `297984`
- original payload bytes: `99702035`
- encoded payload throughput bytes/sec: `769420.14`
- wire throughput bytes/sec: `821561.85`
- peak rss bytes: `290652160`
- peak cpu percent: `102.50`

## Corpus Utility

- measured events: `256`
- exact chunk top-1: `1.000000`
- exact chunk top-5: `1.000000`
- same file top-1: `1.000000`
- same file top-5: `1.000000`
- mean reciprocal rank: `1.000000`

## Source Cache

- lookups: `297984`
- hits: `233628`
- misses: `64356`
- embeddings skipped: `233628`
- cache read ns: `4076640566`
- cache write ns: `23704800969`

## Lane Profiles

- server: bl0_copy=`297984`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`
- client: bl0_copy=`297984`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`

## Source Families

- server: text=`297984`, json=`0`, binary=`0`, unknown=`0`
- client: text=`297984`, json=`0`, binary=`0`, unknown=`0`

## Residual Modes

- server: none=`297984`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
- client: none=`297984`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
