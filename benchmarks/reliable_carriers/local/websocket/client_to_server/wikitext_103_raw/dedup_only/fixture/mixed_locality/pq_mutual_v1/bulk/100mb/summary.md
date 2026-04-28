# Benchmark Summary

- case: `local:websocket:client_to_server:wikitext_103_raw:dedup_only:fixture:mixed_locality:pq_mutual_v1:bulk:native_rust:100mb`
- optimization: `dedup_only`
- client runtime: `native_rust`
- input corpus fingerprint: `8048a14e1542c3b87ee7ae46f5e0528fd17ac00379212dd43177b32f9b014ba6`
- input corpus files: `24209`
- input corpus chunks: `65538`
- target payload bytes: `100000000`
- original payload bytes: `99702316`
- actual payload bytes: `100000216`
- overshoot bytes: `216`
- protected wire bytes: `106704641`
- payload savings vs original: `-0.2988%`
- wire savings vs original: `-7.0232%`
- wire overhead vs encoded: `6.7044%`
- direct connectivity verified: `true`
- build profile: `release`
- sharded: `false`

## Server

- records: `372247`
- vector records: `297900`
- original payload bytes: `99702316`
- encoded payload throughput bytes/sec: `653322.76`
- wire throughput bytes/sec: `697124.20`
- peak rss bytes: `225935360`
- peak cpu percent: `93.00`

## Client

- records: `372247`
- vector records: `297900`
- original payload bytes: `99702316`
- encoded payload throughput bytes/sec: `653343.56`
- wire throughput bytes/sec: `697146.39`
- peak rss bytes: `225935360`
- peak cpu percent: `93.00`

## Lane Profiles

- server: bl0_copy=`297900`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`
- client: bl0_copy=`297900`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`

## Source Families

- server: text=`297900`, json=`0`, binary=`0`, unknown=`0`
- client: text=`297900`, json=`0`, binary=`0`, unknown=`0`

## Residual Modes

- server: none=`297900`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
- client: none=`297900`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
