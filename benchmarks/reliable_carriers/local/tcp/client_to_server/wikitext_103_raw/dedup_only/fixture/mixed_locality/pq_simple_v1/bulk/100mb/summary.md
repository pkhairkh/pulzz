# Benchmark Summary

- case: `local:tcp:client_to_server:wikitext_103_raw:dedup_only:fixture:mixed_locality:pq_simple_v1:bulk:native_rust:100mb`
- optimization: `dedup_only`
- client runtime: `native_rust`
- input corpus fingerprint: `8048a14e1542c3b87ee7ae46f5e0528fd17ac00379212dd43177b32f9b014ba6`
- input corpus files: `24209`
- input corpus chunks: `65538`
- target payload bytes: `100000000`
- original payload bytes: `99702156`
- actual payload bytes: `100000108`
- overshoot bytes: `108`
- protected wire bytes: `106717655`
- payload savings vs original: `-0.2988%`
- wire savings vs original: `-7.0365%`
- wire overhead vs encoded: `6.7175%`
- direct connectivity verified: `true`
- build profile: `release`
- sharded: `false`

## Server

- records: `372976`
- vector records: `297952`
- original payload bytes: `99702156`
- encoded payload throughput bytes/sec: `921279.78`
- wire throughput bytes/sec: `983167.11`
- peak rss bytes: `231243776`
- peak cpu percent: `102.10`

## Client

- records: `372976`
- vector records: `297952`
- original payload bytes: `99702156`
- encoded payload throughput bytes/sec: `921315.63`
- wire throughput bytes/sec: `983205.37`
- peak rss bytes: `231112704`
- peak cpu percent: `101.20`

## Lane Profiles

- server: bl0_copy=`297952`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`
- client: bl0_copy=`297952`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`

## Source Families

- server: text=`297952`, json=`0`, binary=`0`, unknown=`0`
- client: text=`297952`, json=`0`, binary=`0`, unknown=`0`

## Residual Modes

- server: none=`297952`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
- client: none=`297952`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
