# TASKS.md

## Status

**220 tests pass, 0 failures.** Wire savings achieved: +36.7% on high-locality
at batch_size=50.

## Completed work

### Wave 17 (the breakthrough) ✅
- Updated eval harness to support batched emission (`run_evaluation_batched`)
- Added `eval [output_dir] [batch_size]` CLI
- Achieved POSITIVE wire savings on the eval benchmark
- Saved full benchmark results to `benchmarks/eval_wave17/`

### Wave 13 (batching) ✅
- `BatchEnvelope` record type (discriminant 21)
- `emit_batch<I>()` on server; `apply_batch_envelope()` on client
- Whole-batch zstd compression

### Wave 12 (PST authoritative) ✅
- PST short-circuits heuristic planner at ≥ 0.9 confidence

### Wave 8 (PST wiring) ✅
- PST trained on every emission; UCB1 bandit records outcomes

### Waves 1-2 (foundation) ✅
- PQC transcript-binding property test (298 mutations)
- WASM parity, fuzz harness (61,440 cases)
- UCB1 bandit, 128-bit content_hash, neuroscience vocabulary removed

## Remaining work

### R1: Use batched emission in production code paths

The eval harness now supports batched emission, but the `serve` and `bench`
commands still use per-item emission. Update these to use `emit_batch` for
workloads that benefit from batching.

**Steps:**
- [ ] Update `server::serve` to accumulate items and emit batches
- [ ] Update `server::bench` to support batched mode
- [ ] Add batch_size configuration to TransportSessionConfig

### R2: Dead-code physical deletion (5-7 day sprint)

552 Transform references + ~400 Assembly/Schema/Episode references across 16
files. See `docs/wave14_deferral.md` for the 5-phase plan.

### R3: PST-driven batch emission

When the PST predicts DirectState with high confidence for the next N
emissions, accumulate them into a batch instead of emitting individually.
This combines Wave 12 (PST short-circuit) with Wave 13 (batching).

### R4: Remote PQC benchmark (24 cases)

The 24 remote PQC benchmark cases still fail with `No such file or directory`.
Requires a remote server endpoint. Local fallback exists.

### R5: Batch size auto-tuning

Currently batch_size is a manual parameter. Add auto-tuning that picks the
optimal batch size based on observed payload sizes and compression ratios.
