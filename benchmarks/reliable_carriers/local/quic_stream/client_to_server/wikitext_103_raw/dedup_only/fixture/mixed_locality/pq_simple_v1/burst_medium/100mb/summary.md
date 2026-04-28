# Benchmark Summary

- case: `local:quic_stream:client_to_server:wikitext_103_raw:dedup_only:fixture:mixed_locality:pq_simple_v1:burst_medium:native_rust:100mb`
- optimization: `dedup_only`
- client runtime: `native_rust`
- input corpus fingerprint: `8048a14e1542c3b87ee7ae46f5e0528fd17ac00379212dd43177b32f9b014ba6`
- input corpus files: `24209`
- input corpus chunks: `65538`
- target payload bytes: `100000000`
- original payload bytes: `99702442`
- actual payload bytes: `100000345`
- overshoot bytes: `345`
- protected wire bytes: `106775858`
- payload savings vs original: `-0.2988%`
- wire savings vs original: `-7.0945%`
- wire overhead vs encoded: `6.7755%`
- direct connectivity verified: `true`
- build profile: `release`
- sharded: `false`

## Server

- records: `372930`
- vector records: `297903`
- original payload bytes: `99702442`
- encoded payload throughput bytes/sec: `857629.56`
- wire throughput bytes/sec: `915738.16`
- peak rss bytes: `224952320`
- peak cpu percent: `102.60`

## Client

- records: `372930`
- vector records: `297903`
- original payload bytes: `99702442`
- encoded payload throughput bytes/sec: `857617.58`
- wire throughput bytes/sec: `915725.37`
- peak rss bytes: `224952320`
- peak cpu percent: `102.60`

## Lane Profiles

- server: bl0_copy=`297903`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`
- client: bl0_copy=`297903`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`

## Source Families

- server: text=`297903`, json=`0`, binary=`0`, unknown=`0`
- client: text=`297903`, json=`0`, binary=`0`, unknown=`0`

## Residual Modes

- server: none=`297903`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
- client: none=`297903`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
