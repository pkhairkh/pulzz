# Benchmark Summary

- case: `local:tcp:client_to_server:wikitext_103_raw:dedup_only:fixture:mixed_locality:pq_simple_v1:burst_medium:native_rust:100mb`
- optimization: `dedup_only`
- client runtime: `native_rust`
- input corpus fingerprint: `8048a14e1542c3b87ee7ae46f5e0528fd17ac00379212dd43177b32f9b014ba6`
- input corpus files: `24209`
- input corpus chunks: `65538`
- target payload bytes: `100000000`
- original payload bytes: `99702252`
- actual payload bytes: `100000055`
- overshoot bytes: `55`
- protected wire bytes: `106762487`
- payload savings vs original: `-0.2987%`
- wire savings vs original: `-7.0813%`
- wire overhead vs encoded: `6.7624%`
- direct connectivity verified: `true`
- build profile: `release`
- sharded: `false`

## Server

- records: `372201`
- vector records: `297803`
- original payload bytes: `99702252`
- encoded payload throughput bytes/sec: `807355.61`
- wire throughput bytes/sec: `861952.46`
- peak rss bytes: `218021888`
- peak cpu percent: `101.10`

## Client

- records: `372201`
- vector records: `297803`
- original payload bytes: `99702252`
- encoded payload throughput bytes/sec: `807358.06`
- wire throughput bytes/sec: `861955.07`
- peak rss bytes: `218021888`
- peak cpu percent: `101.10`

## Lane Profiles

- server: bl0_copy=`297803`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`
- client: bl0_copy=`297803`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`

## Source Families

- server: text=`297803`, json=`0`, binary=`0`, unknown=`0`
- client: text=`297803`, json=`0`, binary=`0`, unknown=`0`

## Residual Modes

- server: none=`297803`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
- client: none=`297803`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
