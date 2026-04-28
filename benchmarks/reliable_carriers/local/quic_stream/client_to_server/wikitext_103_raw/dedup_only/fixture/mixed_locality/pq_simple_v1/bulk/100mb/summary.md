# Benchmark Summary

- case: `local:quic_stream:client_to_server:wikitext_103_raw:dedup_only:fixture:mixed_locality:pq_simple_v1:bulk:native_rust:100mb`
- optimization: `dedup_only`
- client runtime: `native_rust`
- input corpus fingerprint: `8048a14e1542c3b87ee7ae46f5e0528fd17ac00379212dd43177b32f9b014ba6`
- input corpus files: `24209`
- input corpus chunks: `65538`
- target payload bytes: `100000000`
- original payload bytes: `99702400`
- actual payload bytes: `100000219`
- overshoot bytes: `219`
- protected wire bytes: `106711808`
- payload savings vs original: `-0.2987%`
- wire savings vs original: `-7.0303%`
- wire overhead vs encoded: `6.7116%`
- direct connectivity verified: `true`
- build profile: `release`
- sharded: `false`

## Server

- records: `372645`
- vector records: `297819`
- original payload bytes: `99702400`
- encoded payload throughput bytes/sec: `816550.09`
- wire throughput bytes/sec: `871353.45`
- peak rss bytes: `228458496`
- peak cpu percent: `105.70`

## Client

- records: `372645`
- vector records: `297819`
- original payload bytes: `99702400`
- encoded payload throughput bytes/sec: `816535.89`
- wire throughput bytes/sec: `871338.30`
- peak rss bytes: `228737024`
- peak cpu percent: `103.10`

## Lane Profiles

- server: bl0_copy=`297819`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`
- client: bl0_copy=`297819`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`

## Source Families

- server: text=`297819`, json=`0`, binary=`0`, unknown=`0`
- client: text=`297819`, json=`0`, binary=`0`, unknown=`0`

## Residual Modes

- server: none=`297819`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
- client: none=`297819`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
