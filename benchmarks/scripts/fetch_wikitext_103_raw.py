#!/usr/bin/env python3

import argparse
import hashlib
import json
import sys
import urllib.request
from pathlib import Path

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ModuleNotFoundError as exc:  # pragma: no cover - runtime dependency guard
    raise SystemExit(
        "pyarrow is required to materialize the wikitext benchmark corpus. "
        "Install it with `python3 -m pip install pyarrow` and rerun."
    ) from exc


ROOT = Path(__file__).resolve().parents[2]
CORPUS_DIR = ROOT / "benchmarks" / "input_corpora" / "wikitext_103_raw"
MANIFEST_PATH = CORPUS_DIR / "manifest.json"
CHUNKS_PATH = CORPUS_DIR / "chunks.jsonl"
DATASET = "Salesforce/wikitext"
CONFIG = "wikitext-103-raw-v1"
REVISION = "b08601e04326c79dfdd32d625aee71d232d685c3"
SPLIT = "train"
TARGET_CHUNK_COUNT = 65_536
CHUNK_CHAR_COUNT = 384
CHUNK_CHAR_STRIDE = 256
CHUNK_MIN_CHARS = 96
USER_AGENT = "pulzz-bench/1.0 (wikitext benchmark corpus fetcher)"
TREE_URL = (
    f"https://huggingface.co/api/datasets/{DATASET}/tree/{REVISION}/{CONFIG}"
    "?recursive=0&expand=1"
)
RESOLVE_BASE = f"https://huggingface.co/datasets/{DATASET}/resolve/{REVISION}/"
SOURCE_URL = f"https://huggingface.co/datasets/{DATASET}/tree/{REVISION}/{CONFIG}"


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def split_text_into_chunks(text: str) -> list[str]:
    trimmed = text.strip()
    if not trimmed:
        return []
    if len(trimmed) < CHUNK_MIN_CHARS:
        return []

    chunks: list[str] = []
    start = 0
    while True:
        end = min(start + CHUNK_CHAR_COUNT, len(trimmed))
        chunk = trimmed[start:end].strip()
        if len(chunk) >= CHUNK_MIN_CHARS:
            chunks.append(chunk)
        if end == len(trimmed):
            break
        start += CHUNK_CHAR_STRIDE
        if start >= len(trimmed):
            break

    if not chunks:
        chunks.append(trimmed)
    return chunks


def huggingface_json(url: str) -> object:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.load(response)


def download_bytes(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=300) as response:
        return response.read()


def parquet_files_for_train_split() -> list[str]:
    entries = huggingface_json(TREE_URL)
    if not isinstance(entries, list):
        raise SystemExit(f"unexpected Hugging Face tree response for {TREE_URL}")
    parquet_paths = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        path = entry.get("path")
        if (
            isinstance(path, str)
            and path.startswith(f"{CONFIG}/{SPLIT}-")
            and path.endswith(".parquet")
        ):
            parquet_paths.append(path)
    parquet_paths.sort()
    if not parquet_paths:
        raise SystemExit(f"no parquet files found for {DATASET} {CONFIG} {SPLIT}")
    return parquet_paths


def iter_streamed_rows(parquet_paths: list[str]):
    for parquet_index, parquet_path in enumerate(parquet_paths):
        download_url = RESOLVE_BASE + parquet_path
        print(
            f"downloading {parquet_path} ({parquet_index + 1}/{len(parquet_paths)})",
            file=sys.stderr,
        )
        parquet_bytes = download_bytes(download_url)
        parquet_file = pq.ParquetFile(pa.BufferReader(parquet_bytes))
        for batch in parquet_file.iter_batches(batch_size=1024, columns=["text"]):
            column = batch.column(0)
            for row_index in range(batch.num_rows):
                value = column[row_index].as_py()
                if isinstance(value, str):
                    yield value


def build_corpus() -> tuple[dict, list[dict]]:
    parquet_paths = parquet_files_for_train_split()

    manifest_files: list[dict] = []
    chunk_records: list[dict] = []
    chunk_count = 0
    row_count = 0
    file_index = 0

    for row_text in iter_streamed_rows(parquet_paths):
        row_chunks = split_text_into_chunks(row_text)
        if not row_chunks:
            continue

        row_bytes = row_text.encode("utf-8")
        row_sha256 = sha256_hex(row_bytes)
        relative_path = f"hf_wikitext_103_raw/train/row_{file_index:08d}.txt"
        manifest_files.append(
            {
                "relative_path": relative_path,
                "sha256": row_sha256,
                "byte_len": len(row_bytes),
                "chunk_count": len(row_chunks),
                "mime": "text/plain; charset=utf-8",
                "source_url": SOURCE_URL,
            }
        )
        for chunk_index, chunk_text in enumerate(row_chunks):
            chunk_records.append(
                {
                    "file_index": file_index,
                    "chunk_index": chunk_index,
                    "file_sha256": row_sha256,
                    "chunk_sha256": sha256_hex(chunk_text.encode("utf-8")),
                    "text": chunk_text,
                }
            )
            chunk_count += 1
        file_index += 1
        row_count += 1

        if chunk_count >= TARGET_CHUNK_COUNT:
            break

    if chunk_count < TARGET_CHUNK_COUNT:
        raise SystemExit(
            f"materialized only {chunk_count} chunks from {row_count} rows; "
            f"expected at least {TARGET_CHUNK_COUNT}"
        )

    manifest = {
        "kind": "wikitext_103_raw",
        "source": "huggingface_streamed_chunks_v1",
        "dataset": DATASET,
        "config": CONFIG,
        "revision": REVISION,
        "split": SPLIT,
        "chunk_char_count": CHUNK_CHAR_COUNT,
        "chunk_char_stride": CHUNK_CHAR_STRIDE,
        "chunk_min_chars": CHUNK_MIN_CHARS,
        "target_chunk_count": TARGET_CHUNK_COUNT,
        "file_count": len(manifest_files),
        "chunk_count": chunk_count,
        "fingerprint": sha256_hex(
            json.dumps(manifest_files, ensure_ascii=False, separators=(",", ":")).encode(
                "utf-8"
            )
        ),
        "files": manifest_files,
    }
    return manifest, chunk_records


def write_materialized_corpus(manifest: dict, chunk_records: list[dict]) -> None:
    CORPUS_DIR.mkdir(parents=True, exist_ok=True)
    with CHUNKS_PATH.open("w", encoding="utf-8") as chunks_handle:
        for chunk_record in chunk_records:
            chunks_handle.write(json.dumps(chunk_record, ensure_ascii=False) + "\n")
    MANIFEST_PATH.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(
        f"wrote {MANIFEST_PATH} and {CHUNKS_PATH} with "
        f"{manifest['file_count']} rows / {manifest['chunk_count']} chunks",
        file=sys.stderr,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--stream-json",
        action="store_true",
        help="write the pinned benchmark corpus to stdout as a single JSON object",
    )
    parser.add_argument(
        "--materialize",
        action="store_true",
        help="write the pinned benchmark corpus into benchmarks/input_corpora/wikitext_103_raw",
    )
    args = parser.parse_args()

    if args.stream_json == args.materialize:
        parser.error("choose exactly one of --stream-json or --materialize")

    manifest, chunk_records = build_corpus()
    if args.stream_json:
        json.dump(
            {"manifest": manifest, "chunks": chunk_records},
            sys.stdout,
            ensure_ascii=False,
            separators=(",", ":"),
        )
        sys.stdout.write("\n")
    else:
        write_materialized_corpus(manifest, chunk_records)
    return 0


if __name__ == "__main__":
    sys.exit(main())
