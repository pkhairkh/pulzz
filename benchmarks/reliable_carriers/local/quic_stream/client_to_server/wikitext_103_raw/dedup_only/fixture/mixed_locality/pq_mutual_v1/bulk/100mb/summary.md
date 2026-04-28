# Benchmark Summary

- case: `local:quic_stream:client_to_server:wikitext_103_raw:dedup_only:fixture:mixed_locality:pq_mutual_v1:bulk:native_rust:100mb`
- optimization: `dedup_only`
- client runtime: `native_rust`
- input corpus fingerprint: `8048a14e1542c3b87ee7ae46f5e0528fd17ac00379212dd43177b32f9b014ba6`
- input corpus files: `24209`
- input corpus chunks: `65538`
- target payload bytes: `100000000`
- original payload bytes: `99702356`
- actual payload bytes: `100000209`
- overshoot bytes: `209`
- protected wire bytes: `106706596`
- payload savings vs original: `-0.2987%`
- wire savings vs original: `-7.0251%`
- wire overhead vs encoded: `6.7064%`
- direct connectivity verified: `true`
- build profile: `release`
- sharded: `false`

## Server

- records: `372356`
- vector records: `297853`
- original payload bytes: `99702356`
- encoded payload throughput bytes/sec: `870352.05`
- wire throughput bytes/sec: `928721.11`
- peak rss bytes: `262848512`
- peak cpu percent: `106.20`

## Client

- records: `372356`
- vector records: `297853`
- original payload bytes: `99702356`
- encoded payload throughput bytes/sec: `870371.65`
- wire throughput bytes/sec: `928742.02`
- peak rss bytes: `262799360`
- peak cpu percent: `100.50`

## Lane Profiles

- server: bl0_copy=`297853`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`
- client: bl0_copy=`297853`, bl16=`0`, bl32=`0`, bl64=`0`, bl128=`0`, other=`0`

## Source Families

- server: text=`297853`, json=`0`, binary=`0`, unknown=`0`
- client: text=`297853`, json=`0`, binary=`0`, unknown=`0`

## Residual Modes

- server: none=`297853`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
- client: none=`297853`, all_zero=`0`, small_signed_rans=`0`, sparse_positions=`0`, literal_raw=`0`, unknown=`0`
