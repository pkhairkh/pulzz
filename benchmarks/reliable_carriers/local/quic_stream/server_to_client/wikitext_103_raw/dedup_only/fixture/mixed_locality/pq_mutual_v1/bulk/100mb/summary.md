# Benchmark Summary

- case: `local:quic_stream:server_to_client:wikitext_103_raw:dedup_only:fixture:mixed_locality:pq_mutual_v1:bulk:native_rust:100mb`
- optimization: `dedup_only`
- client runtime: `native_rust`
- input corpus fingerprint: `8048a14e1542c3b87ee7ae46f5e0528fd17ac00379212dd43177b32f9b014ba6`
- input corpus files: `24209`
- input corpus chunks: `65538`
- target payload bytes: `100000000`
- original payload bytes: `99702230`
- actual payload bytes: `100000155`
- overshoot bytes: `155`
- protected wire bytes: `106714912`
- payload savings vs original: `-0.2988%`
- wire savings vs original: `-7.0336%`
- wire overhead vs encoded: `6.7147%`
- direct connectivity verified: `true`
- build profile: `release`
- sharded: `false`

## Server

- records: `372821`
- vector records: `297925`
- original payload bytes: `99702230`
- encoded payload throughput bytes/sec: `797367.17`
- wire throughput bytes/sec: `850908.35`
- peak rss bytes: `293568512`
- peak cpu percent: `100.10`

## Client

- records: `372821`
- vector records: `297925`
- original payload bytes: `99702230`
- encoded payload throughput bytes/sec: `797423.65`
- wire throughput bytes/sec: `850968.63`
- peak rss bytes: `293240832`
- peak cpu percent: `104.70`

## Corpus Utility

- measured events: `256`
- exact chunk top-1: `1.000000`
- exact chunk top-5: `1.000000`
- same file top-1: `1.000000`
- same file top-5: `1.000000`
- mean reciprocal rank: `1.000000`

## Source Cache

- lookups: `297925`
- hits: `233650`
- misses: `64275`
- embeddings skipped: `233650`
- cache read ns: `2088507897`
- cache write ns: `19231616888`

## Lane Profiles

- server: bl0_copy=`297925`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`
- client: bl0_copy=`297925`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`

## Source Families

- server: text=`297925`, json=`0`, binary=`0`, unknown=`0`
- client: text=`297925`, json=`0`, binary=`0`, unknown=`0`

## Residual Modes

- server: none=`297925`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
- client: none=`297925`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
