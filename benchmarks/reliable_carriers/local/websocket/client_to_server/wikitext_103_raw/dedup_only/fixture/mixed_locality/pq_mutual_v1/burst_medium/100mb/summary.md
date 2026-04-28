# Benchmark Summary

- case: `local:websocket:client_to_server:wikitext_103_raw:dedup_only:fixture:mixed_locality:pq_mutual_v1:burst_medium:native_rust:100mb`
- optimization: `dedup_only`
- client runtime: `native_rust`
- input corpus fingerprint: `8048a14e1542c3b87ee7ae46f5e0528fd17ac00379212dd43177b32f9b014ba6`
- input corpus files: `24209`
- input corpus chunks: `65538`
- target payload bytes: `100000000`
- original payload bytes: `99702355`
- actual payload bytes: `100000293`
- overshoot bytes: `293`
- protected wire bytes: `106763337`
- payload savings vs original: `-0.2988%`
- wire savings vs original: `-7.0821%`
- wire overhead vs encoded: `6.7630%`
- direct connectivity verified: `true`
- build profile: `release`
- sharded: `false`

## Server

- records: `372235`
- vector records: `297938`
- original payload bytes: `99702355`
- encoded payload throughput bytes/sec: `705184.33`
- wire throughput bytes/sec: `752876.12`
- peak rss bytes: `227246080`
- peak cpu percent: `101.30`

## Client

- records: `372235`
- vector records: `297938`
- original payload bytes: `99702355`
- encoded payload throughput bytes/sec: `705183.24`
- wire throughput bytes/sec: `752874.95`
- peak rss bytes: `227246080`
- peak cpu percent: `101.30`

## Lane Profiles

- server: bl0_copy=`297938`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`
- client: bl0_copy=`297938`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`

## Source Families

- server: text=`297938`, json=`0`, binary=`0`, unknown=`0`
- client: text=`297938`, json=`0`, binary=`0`, unknown=`0`

## Residual Modes

- server: none=`297938`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
- client: none=`297938`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
