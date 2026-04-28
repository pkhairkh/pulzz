# Benchmark Summary

- case: `local:websocket:server_to_client:wikitext_103_raw:dedup_only:fixture:mixed_locality:pq_simple_v1:burst_medium:native_rust:100mb`
- optimization: `dedup_only`
- client runtime: `native_rust`
- input corpus fingerprint: `8048a14e1542c3b87ee7ae46f5e0528fd17ac00379212dd43177b32f9b014ba6`
- input corpus files: `24209`
- input corpus chunks: `65538`
- target payload bytes: `100000000`
- original payload bytes: `99701964`
- actual payload bytes: `100000004`
- overshoot bytes: `4`
- protected wire bytes: `106774190`
- payload savings vs original: `-0.2989%`
- wire savings vs original: `-7.0934%`
- wire overhead vs encoded: `6.7742%`
- direct connectivity verified: `true`
- build profile: `release`
- sharded: `false`

## Server

- records: `372854`
- vector records: `298040`
- original payload bytes: `99701964`
- encoded payload throughput bytes/sec: `616262.74`
- wire throughput bytes/sec: `658009.53`
- peak rss bytes: `269631488`
- peak cpu percent: `87.70`

## Client

- records: `372854`
- vector records: `298040`
- original payload bytes: `99701964`
- encoded payload throughput bytes/sec: `616850.38`
- wire throughput bytes/sec: `658636.97`
- peak rss bytes: `269631488`
- peak cpu percent: `87.70`

## Corpus Utility

- measured events: `256`
- exact chunk top-1: `1.000000`
- exact chunk top-5: `1.000000`
- same file top-1: `1.000000`
- same file top-5: `1.000000`
- mean reciprocal rank: `1.000000`

## Source Cache

- lookups: `298040`
- hits: `233727`
- misses: `64313`
- embeddings skipped: `233727`
- cache read ns: `5156100816`
- cache write ns: `28344695871`

## Lane Profiles

- server: bl0_copy=`298040`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`
- client: bl0_copy=`298040`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`

## Source Families

- server: text=`298040`, json=`0`, binary=`0`, unknown=`0`
- client: text=`298040`, json=`0`, binary=`0`, unknown=`0`

## Residual Modes

- server: none=`298040`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
- client: none=`298040`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
