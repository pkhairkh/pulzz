# Benchmark Summary

- case: `local:tcp:server_to_client:wikitext_103_raw:dedup_only:fixture:mixed_locality:pq_simple_v1:bulk:native_rust:100mb`
- optimization: `dedup_only`
- client runtime: `native_rust`
- input corpus fingerprint: `8048a14e1542c3b87ee7ae46f5e0528fd17ac00379212dd43177b32f9b014ba6`
- input corpus files: `24209`
- input corpus chunks: `65538`
- target payload bytes: `100000000`
- original payload bytes: `99701860`
- actual payload bytes: `100000040`
- overshoot bytes: `40`
- protected wire bytes: `106707831`
- payload savings vs original: `-0.2991%`
- wire savings vs original: `-7.0269%`
- wire overhead vs encoded: `6.7078%`
- direct connectivity verified: `true`
- build profile: `release`
- sharded: `false`

## Server

- records: `372434`
- vector records: `298180`
- original payload bytes: `99701860`
- encoded payload throughput bytes/sec: `756417.98`
- wire throughput bytes/sec: `807156.90`
- peak rss bytes: `274153472`
- peak cpu percent: `100.00`

## Client

- records: `372434`
- vector records: `298180`
- original payload bytes: `99701860`
- encoded payload throughput bytes/sec: `768551.68`
- wire throughput bytes/sec: `820104.50`
- peak rss bytes: `276987904`
- peak cpu percent: `96.90`

## Corpus Utility

- measured events: `256`
- exact chunk top-1: `1.000000`
- exact chunk top-5: `1.000000`
- same file top-1: `1.000000`
- same file top-5: `1.000000`
- mean reciprocal rank: `1.000000`

## Source Cache

- lookups: `298180`
- hits: `233827`
- misses: `64353`
- embeddings skipped: `233827`
- cache read ns: `2726922412`
- cache write ns: `22605269306`

## Lane Profiles

- server: bl0_copy=`298180`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`
- client: bl0_copy=`298180`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`

## Source Families

- server: text=`298180`, json=`0`, binary=`0`, unknown=`0`
- client: text=`298180`, json=`0`, binary=`0`, unknown=`0`

## Residual Modes

- server: none=`298180`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
- client: none=`298180`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
