#!/usr/bin/env python3
"""Build collaborative-doc benchmark corpus from wikitext_103 chunks.

Generates edit traces that simulate collaborative document editing:
- Insert: add a new line
- Delete: remove a line
- Replace: replace a line's content
- Append: append text to an existing line

Three variants:
- trace_1000.jsonl: mixed edits (default)
- trace_1000_high_locality.jsonl: 90% appends (high locality = predictive wins)
- trace_1000_low_locality.jsonl: 90% full replacements (low locality = predictive falls back)

Run:
    python3 benchmarks/collab_doc/build_corpus.py
"""

import json
import random
import os
from pathlib import Path

CORPUS_DIR = Path(__file__).parent / "corpus"
SOURCE = Path(__file__).parent.parent / "input_corpora" / "wikitext_103_raw" / "chunks.jsonl"

def load_chunks(count=200):
    """Load text chunks from the wikitext corpus."""
    chunks = []
    with open(SOURCE) as f:
        for i, line in enumerate(f):
            if i >= count:
                break
            obj = json.loads(line)
            chunks.append(obj["text"])
    return chunks

def make_edit(state, edit_type, rng):
    """Make a single edit against the doc state (list of lines)."""
    if not state:
        # Empty doc: must insert
        return make_insert(state, rng)

    if edit_type == "insert":
        return make_insert(state, rng)
    elif edit_type == "delete":
        idx = rng.randrange(len(state))
        return {"edit_type": "delete", "line_index": idx, "old_content": state[idx], "new_content": ""}
    elif edit_type == "replace":
        idx = rng.randrange(len(state))
        old = state[idx]
        # Replacement: unrelated text
        new = f"Replacement line {rng.randrange(1000000)}"
        return {"edit_type": "replace", "line_index": idx, "old_content": old, "new_content": new}
    elif edit_type == "append":
        idx = rng.randrange(len(state))
        old = state[idx]
        appended = f" suffix_{rng.randrange(1000000)}"
        return {"edit_type": "append", "line_index": idx, "old_content": old, "appended": appended}
    else:
        raise ValueError(f"unknown edit_type: {edit_type}")

def make_insert(state, rng):
    """Make an insert edit."""
    content = f"New line {rng.randrange(1000000)} with some text content"
    idx = rng.randrange(len(state) + 1) if state else 0
    return {"edit_type": "insert", "line_index": idx, "old_content": "", "new_content": content}

def apply_edit(state, edit):
    """Apply an edit to the doc state (mutates state in place)."""
    et = edit["edit_type"]
    idx = edit["line_index"]
    if et == "insert":
        state.insert(idx, edit["new_content"])
    elif et == "delete":
        if idx < len(state):
            state.pop(idx)
    elif et == "replace":
        if idx < len(state):
            state[idx] = edit["new_content"]
    elif et == "append":
        if idx < len(state):
            state[idx] += edit["appended"]
    return state

def generate_trace(chunks, n_edits, locality, seed=42):
    """Generate an edit trace.

    locality: 'mixed', 'high', or 'low'
        - mixed: 25% each of insert/delete/replace/append
        - high: 90% append, 10% other (high locality)
        - low: 90% replace, 10% other (low locality)
    """
    rng = random.Random(seed)
    # Start with a doc built from the first chunk
    initial_text = chunks[0]
    state = initial_text.split("\n")

    trace = []
    for i in range(n_edits):
        r = rng.random()
        if locality == "high":
            if r < 0.90:
                et = "append"
            elif r < 0.93:
                et = "insert"
            elif r < 0.96:
                et = "delete"
            else:
                et = "replace"
        elif locality == "low":
            if r < 0.90:
                et = "replace"
            elif r < 0.93:
                et = "insert"
            elif r < 0.96:
                et = "delete"
            else:
                et = "append"
        else:  # mixed
            if r < 0.25:
                et = "insert"
            elif r < 0.50:
                et = "delete"
            elif r < 0.75:
                et = "replace"
            else:
                et = "append"

        edit = make_edit(state, et, rng)
        apply_edit(state, edit)
        trace.append(edit)

    return trace

def main():
    CORPUS_DIR.mkdir(parents=True, exist_ok=True)
    chunks = load_chunks(count=200)
    print(f"Loaded {len(chunks)} chunks from wikitext_103")

    for locality, filename in [
        ("mixed", "trace_1000.jsonl"),
        ("high", "trace_1000_high_locality.jsonl"),
        ("low", "trace_1000_low_locality.jsonl"),
    ]:
        trace = generate_trace(chunks, n_edits=1000, locality=locality)
        outpath = CORPUS_DIR / filename
        with open(outpath, "w") as f:
            for edit in trace:
                f.write(json.dumps(edit) + "\n")
        # Count edit types
        counts = {}
        for e in trace:
            counts[e["edit_type"]] = counts.get(e["edit_type"], 0) + 1
        print(f"  {filename}: {len(trace)} edits, types: {counts}")

if __name__ == "__main__":
    main()
