#!/usr/bin/env python3
"""
Generate byte_latent_v2 assets for the Rust runtime.

The v2 assets replace the placeholder group-average/repeat manifests with a
real quantized linear codec family:
- shared analysis trunk: 256 -> 192 -> 128
- shared synthesis trunk: 128 -> 192 -> 256
- per-profile latent heads: 128 -> {16,32,64,128}
- centered signed residual coding metadata
- static categorical probability tables for latent codes and residual symbols

Tooling-only dependencies:
- numpy
- optional pyarrow for streamed Wikitext training blocks
"""

from __future__ import annotations

import importlib.util
import json
import math
import random
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / "shared_protocol" / "assets" / "byte_latent"
BLOCK_LEN = 256
TRUNK_1_DIM = 192
TRUNK_2_DIM = 128
LATENT_LEVEL_COUNT = 16
TEXT_BLOCK_TARGET = 4096
JSON_BLOCK_TARGET = 4096
BINARY_BLOCK_TARGET = 2048
SMALL_SIGNED_RANGE = 31

PROFILES = [
    ("bl16", 0x0010, 16),
    ("bl32", 0x0020, 32),
    ("bl64", 0x0040, 64),
    ("bl128", 0x0080, 128),
]


@dataclass
class FamilyTrainingData:
    family_name: str
    blocks: np.ndarray


def load_wikitext_module():
    script_path = ROOT / "benchmarks" / "scripts" / "fetch_wikitext_103_raw.py"
    spec = importlib.util.spec_from_file_location("fetch_wikitext_103_raw", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {script_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def iter_fixed_blocks_from_bytes(data: bytes):
    if not data:
        return
    for start in range(0, len(data), BLOCK_LEN):
        block = np.zeros(BLOCK_LEN, dtype=np.float32)
        chunk = data[start : start + BLOCK_LEN]
        block[: len(chunk)] = np.frombuffer(chunk, dtype=np.uint8).astype(np.float32)
        yield block


def repo_text_blocks(limit: int) -> list[np.ndarray]:
    blocks: list[np.ndarray] = []
    exts = {".md", ".rs", ".toml", ".json", ".txt", ".yml", ".yaml"}
    for path in ROOT.rglob("*"):
        if len(blocks) >= limit:
            break
        if not path.is_file() or path.suffix.lower() not in exts:
            continue
        try:
            data = path.read_bytes()
        except OSError:
            continue
        for block in iter_fixed_blocks_from_bytes(data):
            blocks.append(block)
            if len(blocks) >= limit:
                break
    return blocks


def collect_text_blocks(limit: int) -> list[np.ndarray]:
    try:
        module = load_wikitext_module()
    except Exception:
        return repo_text_blocks(limit)

    blocks: list[np.ndarray] = []
    try:
        parquet_paths = module.parquet_files_for_train_split()
        for row_text in module.iter_streamed_rows(parquet_paths):
            chunks = module.split_text_into_chunks(row_text)
            for chunk in chunks:
                for block in iter_fixed_blocks_from_bytes(chunk.encode("utf-8")):
                    blocks.append(block)
                    if len(blocks) >= limit:
                        return blocks
    except Exception:
        return repo_text_blocks(limit)
    return blocks


def random_ascii_word(rng: random.Random, min_len: int = 3, max_len: int = 14) -> str:
    alphabet = "abcdefghijklmnopqrstuvwxyz"
    length = rng.randint(min_len, max_len)
    return "".join(rng.choice(alphabet) for _ in range(length))


def build_json_document(rng: random.Random) -> str:
    key_pool = [
        "id",
        "name",
        "email",
        "active",
        "score",
        "tags",
        "meta",
        "created_at",
        "path",
        "count",
        "enabled",
        "message",
        "version",
        "flags",
        "items",
    ]
    doc: dict[str, object] = {}
    doc["id"] = rng.randint(1, 10_000_000)
    doc["name"] = random_ascii_word(rng, 4, 12)
    doc["active"] = bool(rng.getrandbits(1))
    doc["score"] = round(rng.random() * 1000.0, 3)
    doc["count"] = rng.randint(0, 5000)
    doc["tags"] = [random_ascii_word(rng, 3, 8) for _ in range(rng.randint(1, 6))]
    doc["flags"] = {
        random_ascii_word(rng, 3, 8): bool(rng.getrandbits(1))
        for _ in range(rng.randint(1, 4))
    }
    doc["items"] = [
        {
            "kind": rng.choice(["user", "group", "team", "job"]),
            "value": random_ascii_word(rng, 4, 10),
            "weight": rng.randint(0, 100),
        }
        for _ in range(rng.randint(1, 4))
    ]
    doc["meta"] = {
        "version": f"v{rng.randint(1,9)}.{rng.randint(0,9)}.{rng.randint(0,9)}",
        "path": "/" + "/".join(random_ascii_word(rng, 2, 8) for _ in range(rng.randint(1, 4))),
        "created_at": f"2026-04-{rng.randint(1,28):02d}T{rng.randint(0,23):02d}:{rng.randint(0,59):02d}:{rng.randint(0,59):02d}Z",
    }
    extra_keys = rng.sample(key_pool, k=rng.randint(0, 4))
    for key in extra_keys:
        doc.setdefault(key, random_ascii_word(rng, 4, 18))
    return json.dumps(doc, separators=(",", ":"), sort_keys=True)


def collect_json_blocks(limit: int) -> list[np.ndarray]:
    rng = random.Random(0xB17E_DA7A)
    blocks: list[np.ndarray] = []
    while len(blocks) < limit:
        document = build_json_document(rng).encode("utf-8")
        for block in iter_fixed_blocks_from_bytes(document):
            blocks.append(block)
            if len(blocks) >= limit:
                break
    return blocks


def collect_binary_blocks(limit: int) -> list[np.ndarray]:
    rng = random.Random(0xB1A5_EED)
    blocks: list[np.ndarray] = []
    headers = [
        b"\x89PNG\r\n\x1a\n",
        b"\xff\xd8\xff\xe0JFIF",
        b"RIFFWEBP",
        b"GIF89a",
        b"%PDF-1.7",
        b"PK\x03\x04",
    ]
    while len(blocks) < limit:
        header = rng.choice(headers)
        payload = bytearray(header)
        pattern = rng.randbytes(rng.randint(8, 32))
        while len(payload) < BLOCK_LEN * 2:
            if rng.random() < 0.65:
                payload.extend(pattern)
            else:
                payload.extend(rng.randbytes(rng.randint(16, 96)))
        for block in iter_fixed_blocks_from_bytes(bytes(payload)):
            blocks.append(block)
            if len(blocks) >= limit:
                break
    return blocks


def ensure_training_matrix(blocks: list[np.ndarray], minimum_blocks: int) -> np.ndarray:
    if len(blocks) < minimum_blocks:
        raise RuntimeError(f"need at least {minimum_blocks} blocks, got {len(blocks)}")
    return np.stack(blocks).astype(np.float32)


def selection_weights(out_dim: int, in_dim: int) -> np.ndarray:
    weights = np.zeros((out_dim, in_dim), dtype=np.float32)
    for index in range(min(out_dim, in_dim)):
        weights[index, index] = 1.0
    return weights


def padding_weights(out_dim: int, in_dim: int) -> np.ndarray:
    weights = np.zeros((out_dim, in_dim), dtype=np.float32)
    for index in range(min(out_dim, in_dim)):
        weights[index, index] = 1.0
    return weights


def quantize_levels(values: np.ndarray) -> list[int]:
    flattened = values.reshape(-1)
    percentiles = np.linspace(2.0, 98.0, LATENT_LEVEL_COUNT)
    raw = np.percentile(flattened, percentiles)
    levels = np.rint(raw).astype(np.int32)
    for index in range(1, len(levels)):
        if levels[index] < levels[index - 1]:
            levels[index] = levels[index - 1]
    if len(set(levels.tolist())) == 1:
        magnitude = max(1, int(np.ceil(np.std(flattened))))
        levels = np.linspace(-magnitude * 4, magnitude * 4, LATENT_LEVEL_COUNT).astype(np.int32)
    return levels.tolist()


def nearest_level_indices(values: np.ndarray, levels: np.ndarray) -> np.ndarray:
    distances = np.abs(values[..., None] - levels[None, None, :])
    return np.argmin(distances, axis=-1).astype(np.int32)


def normalized_probabilities(counts: np.ndarray) -> list[float]:
    counts = counts.astype(np.float64) + 1.0
    counts /= counts.sum()
    return counts.tolist()


def wrap_signed_residual(actual: np.ndarray, predicted: np.ndarray) -> np.ndarray:
    delta = (actual.astype(np.int16) - predicted.astype(np.int16) + 128) % 256 - 128
    return delta.astype(np.int16)


def make_linear_layer(
    weights: np.ndarray,
    bias: np.ndarray,
    clamp_min: int,
    clamp_max: int,
    *,
    apply_relu: bool = False,
    input_zero_point: int = 0,
    output_zero_point: int = 0,
) -> dict:
    max_abs = float(np.max(np.abs(weights))) if weights.size else 0.0
    scale = 1 if max_abs == 0.0 else max(1, int(math.floor(127.0 / max_abs)))
    quantized_weights = np.clip(np.rint(weights * scale), -127, 127).astype(np.int8)
    quantized_bias = np.rint(bias * scale).astype(np.int32)
    return {
        "in_dim": int(weights.shape[1]),
        "out_dim": int(weights.shape[0]),
        "input_zero_point": int(input_zero_point),
        "output_zero_point": int(output_zero_point),
        "divisor": int(scale),
        "apply_relu": bool(apply_relu),
        "clamp_min": int(clamp_min),
        "clamp_max": int(clamp_max),
        "weights": quantized_weights.reshape(-1).tolist(),
        "bias": quantized_bias.reshape(-1).tolist(),
    }


def build_family_manifest(name: str, blocks: np.ndarray) -> dict:
    mean = blocks.mean(axis=0)
    centered = blocks - mean
    _, _, vh = np.linalg.svd(centered, full_matrices=False)
    components192 = vh[:TRUNK_1_DIM].astype(np.float32)

    analysis_trunk = [
        make_linear_layer(
            components192,
            -components192 @ mean,
            clamp_min=-4096,
            clamp_max=4096,
        ),
        make_linear_layer(
            selection_weights(TRUNK_2_DIM, TRUNK_1_DIM),
            np.zeros(TRUNK_2_DIM, dtype=np.float32),
            clamp_min=-4096,
            clamp_max=4096,
        ),
    ]

    synthesis_trunk = [
        make_linear_layer(
            padding_weights(TRUNK_1_DIM, TRUNK_2_DIM),
            np.zeros(TRUNK_1_DIM, dtype=np.float32),
            clamp_min=-4096,
            clamp_max=4096,
        ),
        make_linear_layer(
            components192.T,
            mean,
            clamp_min=0,
            clamp_max=255,
        ),
    ]

    latent128 = centered @ components192[:TRUNK_2_DIM].T
    profiles = []
    residual_counts = np.ones(2 * SMALL_SIGNED_RANGE + 1, dtype=np.int64)

    for profile_name, lane_profile_id, latent_dim in PROFILES:
        levels = np.array(quantize_levels(latent128[:, :latent_dim]), dtype=np.float32)
        code_indices = nearest_level_indices(latent128[:, :latent_dim], levels)
        code_counts = np.bincount(code_indices.reshape(-1), minlength=LATENT_LEVEL_COUNT)

        quantized_latents = levels[code_indices]
        padded_latents = np.zeros((blocks.shape[0], TRUNK_1_DIM), dtype=np.float32)
        padded_latents[:, :latent_dim] = quantized_latents
        predicted = padded_latents @ components192
        predicted = np.clip(np.rint(predicted + mean), 0, 255).astype(np.int16)
        residual = wrap_signed_residual(blocks.astype(np.int16), predicted)

        mask = np.abs(residual) <= SMALL_SIGNED_RANGE
        residual_counts += np.bincount(
            (residual[mask] + SMALL_SIGNED_RANGE).astype(np.int32),
            minlength=2 * SMALL_SIGNED_RANGE + 1,
        )

        profiles.append(
            {
                "profile_name": profile_name,
                "lane_profile_id": lane_profile_id,
                "latent_dim": latent_dim,
                "fsq_levels": [int(level) for level in levels.tolist()],
                "latent_probabilities": normalized_probabilities(code_counts),
                "analysis_head": make_linear_layer(
                    selection_weights(latent_dim, TRUNK_2_DIM),
                    np.zeros(latent_dim, dtype=np.float32),
                    clamp_min=-4096,
                    clamp_max=4096,
                ),
                "synthesis_head": make_linear_layer(
                    padding_weights(TRUNK_2_DIM, latent_dim),
                    np.zeros(TRUNK_2_DIM, dtype=np.float32),
                    clamp_min=-4096,
                    clamp_max=4096,
                ),
            }
        )

    return {
        "format_version": 2,
        "family_name": name,
        "block_len": BLOCK_LEN,
        "residual": {
            "all_zero_tag": 0,
            "small_signed_rans_tag": 1,
            "sparse_positions_tag": 2,
            "literal_raw_tag": 3,
            "small_range": SMALL_SIGNED_RANGE,
            "sparse_threshold_percent": 4.0,
            "small_signed_probabilities": normalized_probabilities(residual_counts),
        },
        "analysis_trunk": analysis_trunk,
        "synthesis_trunk": synthesis_trunk,
        "profiles": profiles,
    }


def gather_training_data() -> list[FamilyTrainingData]:
    return [
        FamilyTrainingData(
            "text_v2",
            ensure_training_matrix(collect_text_blocks(TEXT_BLOCK_TARGET), minimum_blocks=1024),
        ),
        FamilyTrainingData(
            "json_v2",
            ensure_training_matrix(collect_json_blocks(JSON_BLOCK_TARGET), minimum_blocks=1024),
        ),
        FamilyTrainingData(
            "binary_v2",
            ensure_training_matrix(collect_binary_blocks(BINARY_BLOCK_TARGET), minimum_blocks=512),
        ),
    ]


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for training_data in gather_training_data():
        manifest = build_family_manifest(training_data.family_name, training_data.blocks)
        out_path = OUT_DIR / f"{training_data.family_name}.json"
        out_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        print(out_path)


if __name__ == "__main__":
    sys.exit(main())
