# Wave 17 Benchmark Results

**Date:** 2026-08-13
**Corpus:** wikitext_103_raw (mixed JSON/text/binary, ~80 B/item average)
**Workloads:** high_locality, mixed_locality, low_locality (160 events each)
**Exact round-trip rate:** 1.000000 (all configurations)

## Summary table

| Configuration | high_locality | mixed_locality | low_locality |
|---|---|---|---|
| Per-item (baseline) | **-105.4%** | **-92.6%** | **-87.5%** |
| Batched (batch_size=10) | **+20.7%** | **+19.9%** | **+17.4%** |
| Batched (batch_size=20) | **+30.9%** | **+29.3%** | **+26.7%** |
| Batched (batch_size=50) | **+36.7%** | **+35.6%** | **+32.5%** |

**The batching path achieves POSITIVE wire savings on the actual eval benchmark.**

## Detailed results

### Per-item emission (baseline — the old default)

| Workload | Original bytes | Wire bytes | Wire savings |
|---|---|---|---|
| high_locality | 12,855 | 26,408 | -105.4% |
| mixed_locality | 12,311 | 23,711 | -92.6% |
| low_locality | 11,478 | 21,522 | -87.5% |

Negative savings because per-item AEAD tag (16B) + record header (32B) +
transport envelope (16B) + source descriptor (30B) = ~94B overhead per item
on ~80B average payloads.

### Batched emission (batch_size=10)

| Workload | Original bytes | Wire bytes | Wire savings |
|---|---|---|---|
| high_locality | 12,855 | 10,189 | **+20.7%** |
| mixed_locality | 12,311 | 9,857 | **+19.9%** |
| low_locality | 11,478 | 9,486 | **+17.4%** |

### Batched emission (batch_size=20)

| Workload | Original bytes | Wire bytes | Wire savings |
|---|---|---|---|
| high_locality | 12,855 | 8,882 | **+30.9%** |
| mixed_locality | 12,311 | 8,707 | **+29.3%** |
| low_locality | 11,478 | 8,419 | **+26.7%** |

### Batched emission (batch_size=50)

| Workload | Original bytes | Wire bytes | Wire savings |
|---|---|---|---|
| high_locality | 12,855 | 8,139 | **+36.7%** |
| mixed_locality | 12,311 | 7,934 | **+35.6%** |
| low_locality | 11,478 | 7,751 | **+32.5%** |

## How to reproduce

```bash
# Per-item (baseline)
cargo run --release -p server -- eval artifacts/eval_per_item

# Batched (batch_size=10)
cargo run --release -p server -- eval artifacts/eval_batched_10 10

# Batched (batch_size=20)
cargo run --release -p server -- eval artifacts/eval_batched_20 20

# Batched (batch_size=50)
cargo run --release -p server -- eval artifacts/eval_batched_50 50
```

## How batching works

1. **Amortize overhead:** N items share one AEAD tag, one record header, one
   transport envelope. Per-item overhead drops from ~94B to ~94B/N.
2. **Cross-item compression:** The entire batch envelope is compressed as a
   single zstd stream, exploiting repeated JSON keys, identical field names,
   and similar values across items — redundancy that per-item compression
   cannot capture.

## Data integrity

All configurations achieve **exact round-trip rate = 1.000000** — every item
decoded on the client matches the original bytes.
