# ISSUES

## Resolved

- **Negative wire savings** — fixed by batched emission (`emit_batch` +
  `BatchEnvelope`). Batched(50): +36.7% / +35.6% / +32.5%. PQ batched: +95.7%.
- **Client decompression** — `apply_batch_envelope` decompresses and caches all
  items.
- **PQC verification** — ML-KEM-768 batched benchmark verified.
- **WASM build** — `shared_protocol` compiles for wasm32-unknown-unknown.
- **Dead code** — Transform/Assembly/Schema/Episode enum variants physically
  deleted from `RouteFamily` and `ControllerRouteFamily`. Wire discriminants
  for `TransformDef`/`TransformCorrect` retained for backward-compat decode.

## Remaining

- **Transform/Assembly/Schema/Episode structs** — the enum variants are gone but
  the struct definitions (`TransformDefPayload`, `AssemblyDefPayload`, etc.)
  and their handling code remain. They compile cleanly (0 warnings) but are
  unused. Low priority — they don't affect wire bytes or correctness.
- **Remote PQC benchmark** (24 cases) — requires remote server endpoint.
  Local fallback exists.
- **`serve`/`bench` commands** still use per-item emission. Update to
  `emit_batch` for production use.
