# Benchmark Summary

- case: `local:tcp:client_to_server:wikitext_103_raw:dedup_only:fixture:mixed_locality:pq_mutual_v1:burst_medium:native_rust:100mb`
- optimization: `dedup_only`
- client runtime: `native_rust`
- input corpus fingerprint: `8048a14e1542c3b87ee7ae46f5e0528fd17ac00379212dd43177b32f9b014ba6`
- input corpus files: `24209`
- input corpus chunks: `65538`
- target payload bytes: `100000000`
- original payload bytes: `99702154`
- actual payload bytes: `100000094`
- overshoot bytes: `94`
- protected wire bytes: `106770189`
- payload savings vs original: `-0.2988%`
- wire savings vs original: `-7.0891%`
- wire overhead vs encoded: `6.7701%`
- direct connectivity verified: `true`
- build profile: `release`
- sharded: `false`

## Server

- records: `372629`
- vector records: `297940`
- original payload bytes: `99702154`
- encoded payload throughput bytes/sec: `750942.48`
- wire throughput bytes/sec: `801781.95`
- peak rss bytes: `202080256`
- peak cpu percent: `100.30`

## Client

- records: `372629`
- vector records: `297940`
- original payload bytes: `99702154`
- encoded payload throughput bytes/sec: `750944.94`
- wire throughput bytes/sec: `801784.58`
- peak rss bytes: `202080256`
- peak cpu percent: `100.30`

## Lane Profiles

- server: bl0_copy=`297940`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`
- client: bl0_copy=`297940`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`

## Source Families

- server: text=`297940`, json=`0`, binary=`0`, unknown=`0`
- client: text=`297940`, json=`0`, binary=`0`, unknown=`0`

## Residual Modes

- server: none=`297940`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
- client: none=`297940`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
