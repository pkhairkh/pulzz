# Benchmark Summary

- case: `local:tcp:client_to_server:wikitext_103_raw:dedup_only:fixture:mixed_locality:pq_mutual_v1:bulk:native_rust:100mb`
- optimization: `dedup_only`
- client runtime: `native_rust`
- input corpus fingerprint: `8048a14e1542c3b87ee7ae46f5e0528fd17ac00379212dd43177b32f9b014ba6`
- input corpus files: `24209`
- input corpus chunks: `65538`
- target payload bytes: `100000000`
- original payload bytes: `99701911`
- actual payload bytes: `100000000`
- overshoot bytes: `0`
- protected wire bytes: `106714019`
- payload savings vs original: `-0.2990%`
- wire savings vs original: `-7.0331%`
- wire overhead vs encoded: `6.7140%`
- direct connectivity verified: `true`
- build profile: `release`
- sharded: `false`

## Server

- records: `372780`
- vector records: `298089`
- original payload bytes: `99701911`
- encoded payload throughput bytes/sec: `794746.59`
- wire throughput bytes/sec: `848106.03`
- peak rss bytes: `210419712`
- peak cpu percent: `100.00`

## Client

- records: `372780`
- vector records: `298089`
- original payload bytes: `99701911`
- encoded payload throughput bytes/sec: `794864.36`
- wire throughput bytes/sec: `848231.71`
- peak rss bytes: `210567168`
- peak cpu percent: `100.00`

## Lane Profiles

- server: bl0_copy=`298089`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`
- client: bl0_copy=`298089`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`

## Source Families

- server: text=`298089`, json=`0`, binary=`0`, unknown=`0`
- client: text=`298089`, json=`0`, binary=`0`, unknown=`0`

## Residual Modes

- server: none=`298089`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
- client: none=`298089`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
