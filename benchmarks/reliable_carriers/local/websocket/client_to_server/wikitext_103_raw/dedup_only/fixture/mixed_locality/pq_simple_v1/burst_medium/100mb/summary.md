# Benchmark Summary

- case: `local:websocket:client_to_server:wikitext_103_raw:dedup_only:fixture:mixed_locality:pq_simple_v1:burst_medium:native_rust:100mb`
- optimization: `dedup_only`
- client runtime: `native_rust`
- input corpus fingerprint: `8048a14e1542c3b87ee7ae46f5e0528fd17ac00379212dd43177b32f9b014ba6`
- input corpus files: `24209`
- input corpus chunks: `65538`
- target payload bytes: `100000000`
- original payload bytes: `99702415`
- actual payload bytes: `100000199`
- overshoot bytes: `199`
- protected wire bytes: `106771968`
- payload savings vs original: `-0.2987%`
- wire savings vs original: `-7.0907%`
- wire overhead vs encoded: `6.7718%`
- direct connectivity verified: `true`
- build profile: `release`
- sharded: `false`

## Server

- records: `372722`
- vector records: `297784`
- original payload bytes: `99702415`
- encoded payload throughput bytes/sec: `910453.82`
- wire throughput bytes/sec: `972107.52`
- peak rss bytes: `248823808`
- peak cpu percent: `101.10`

## Client

- records: `372722`
- vector records: `297784`
- original payload bytes: `99702415`
- encoded payload throughput bytes/sec: `910456.07`
- wire throughput bytes/sec: `972109.93`
- peak rss bytes: `248823808`
- peak cpu percent: `101.10`

## Lane Profiles

- server: bl0_copy=`297784`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`
- client: bl0_copy=`297784`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`

## Source Families

- server: text=`297784`, json=`0`, binary=`0`, unknown=`0`
- client: text=`297784`, json=`0`, binary=`0`, unknown=`0`

## Residual Modes

- server: none=`297784`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
- client: none=`297784`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
