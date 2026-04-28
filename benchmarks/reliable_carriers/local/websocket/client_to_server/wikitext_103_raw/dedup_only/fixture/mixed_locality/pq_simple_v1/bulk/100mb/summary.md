# Benchmark Summary

- case: `local:websocket:client_to_server:wikitext_103_raw:dedup_only:fixture:mixed_locality:pq_simple_v1:bulk:native_rust:100mb`
- optimization: `dedup_only`
- client runtime: `native_rust`
- input corpus fingerprint: `8048a14e1542c3b87ee7ae46f5e0528fd17ac00379212dd43177b32f9b014ba6`
- input corpus files: `24209`
- input corpus chunks: `65538`
- target payload bytes: `100000000`
- original payload bytes: `99702348`
- actual payload bytes: `100000243`
- overshoot bytes: `243`
- protected wire bytes: `106709060`
- payload savings vs original: `-0.2988%`
- wire savings vs original: `-7.0276%`
- wire overhead vs encoded: `6.7088%`
- direct connectivity verified: `true`
- build profile: `release`
- sharded: `false`

## Server

- records: `372491`
- vector records: `297895`
- original payload bytes: `99702348`
- encoded payload throughput bytes/sec: `768995.02`
- wire throughput bytes/sec: `820585.36`
- peak rss bytes: `236453888`
- peak cpu percent: `100.10`

## Client

- records: `372491`
- vector records: `297895`
- original payload bytes: `99702348`
- encoded payload throughput bytes/sec: `769019.82`
- wire throughput bytes/sec: `820611.82`
- peak rss bytes: `235569152`
- peak cpu percent: `100.10`

## Lane Profiles

- server: bl0_copy=`297895`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`
- client: bl0_copy=`297895`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`

## Source Families

- server: text=`297895`, json=`0`, binary=`0`, unknown=`0`
- client: text=`297895`, json=`0`, binary=`0`, unknown=`0`

## Residual Modes

- server: none=`297895`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
- client: none=`297895`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
