# Benchmark Summary

- case: `local:quic_stream:client_to_server:wikitext_103_raw:dedup_only:fixture:mixed_locality:pq_mutual_v1:burst_medium:native_rust:100mb`
- optimization: `dedup_only`
- client runtime: `native_rust`
- input corpus fingerprint: `8048a14e1542c3b87ee7ae46f5e0528fd17ac00379212dd43177b32f9b014ba6`
- input corpus files: `24209`
- input corpus chunks: `65538`
- target payload bytes: `100000000`
- original payload bytes: `99702099`
- actual payload bytes: `100000046`
- overshoot bytes: `46`
- protected wire bytes: `106764404`
- payload savings vs original: `-0.2988%`
- wire savings vs original: `-7.0834%`
- wire overhead vs encoded: `6.7644%`
- direct connectivity verified: `true`
- build profile: `release`
- sharded: `false`

## Server

- records: `372308`
- vector records: `297947`
- original payload bytes: `99702099`
- encoded payload throughput bytes/sec: `711397.18`
- wire throughput bytes/sec: `759518.61`
- peak rss bytes: `220119040`
- peak cpu percent: `101.60`

## Client

- records: `372308`
- vector records: `297947`
- original payload bytes: `99702099`
- encoded payload throughput bytes/sec: `711395.81`
- wire throughput bytes/sec: `759517.15`
- peak rss bytes: `220119040`
- peak cpu percent: `101.60`

## Lane Profiles

- server: bl0_copy=`297947`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`
- client: bl0_copy=`297947`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`

## Source Families

- server: text=`297947`, json=`0`, binary=`0`, unknown=`0`
- client: text=`297947`, json=`0`, binary=`0`, unknown=`0`

## Residual Modes

- server: none=`297947`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
- client: none=`297947`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
