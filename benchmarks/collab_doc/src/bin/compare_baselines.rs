//! Run naive + predictive baselines on all 3 corpus traces and compare.
//!
//! Usage:
//!   cargo run --bin compare_baselines -p pulzz-bench-collab --release
//!
//! Output:
//!   Prints a comparison table + writes results JSON files to
//!   benchmarks/collab_doc/results/.

use pulzz_bench_collab::{Edit, DocState, naive_record_for_edit, predictive_record_for_edit, total_wire_bytes};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Serialize, Deserialize)]
struct BaselineResult {
    trace: String,
    path: String,
    n_edits: usize,
    naive_total_bytes: usize,
    predictive_total_bytes: usize,
    naive_avg_bytes_per_edit: f64,
    predictive_avg_bytes_per_edit: f64,
    reduction_pct: f64,
    edit_type_counts: std::collections::HashMap<String, usize>,
}

fn load_trace(path: &str) -> Vec<Edit> {
    let content = fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("failed to read {path}: {e}"));
    content
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_str::<Edit>(l).expect("failed to parse edit"))
        .collect()
}

fn run_baselines(trace_name: &str, trace_path: &str) -> BaselineResult {
    let edits = load_trace(trace_path);
    let n_edits = edits.len();

    // Track doc state to know old_content for predictive replace decisions.
    // Start with the same initial doc the corpus generator used (first chunk
    // split into lines). For simplicity, we start empty and let inserts build
    // it up — the old_content for appends will be available from the state.
    let mut state = DocState::default();

    let mut naive_total = 0usize;
    let mut predictive_total = 0usize;
    let mut edit_type_counts: std::collections::HashMap<String, usize> = Default::default();

    for edit in &edits {
        // Track old content for predictive decisions
        let old_content = match edit {
            Edit::Replace { line_index, .. } | Edit::Append { line_index, .. } => {
                state.line(*line_index).map(|s| s.to_string())
            }
            _ => None,
        };

        let naive_rec = naive_record_for_edit(edit, old_content.as_deref());
        let pred_rec = predictive_record_for_edit(edit, old_content.as_deref());

        naive_total += total_wire_bytes(&[naive_rec]);
        predictive_total += total_wire_bytes(&[pred_rec]);

        // Apply edit to state
        state.apply(edit);

        // Count edit types
        let et = match edit {
            Edit::Insert { .. } => "insert",
            Edit::Delete { .. } => "delete",
            Edit::Replace { .. } => "replace",
            Edit::Append { .. } => "append",
        };
        *edit_type_counts.entry(et.to_string()).or_insert(0) += 1;
    }

    let reduction_pct = if naive_total > 0 {
        (naive_total as f64 - predictive_total as f64) / naive_total as f64 * 100.0
    } else {
        0.0
    };

    BaselineResult {
        trace: trace_name.to_string(),
        path: trace_path.to_string(),
        n_edits,
        naive_total_bytes: naive_total,
        predictive_total_bytes: predictive_total,
        naive_avg_bytes_per_edit: naive_total as f64 / n_edits as f64,
        predictive_avg_bytes_per_edit: predictive_total as f64 / n_edits as f64,
        reduction_pct,
        edit_type_counts,
    }
}

fn main() {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let corpus_dir = base.join("corpus");
    let results_dir = base.join("results");
    fs::create_dir_all(&results_dir).expect("failed to create results dir");

    let traces = [
        ("mixed", "trace_1000.jsonl", "comparison_mixed.json"),
        ("high_locality", "trace_1000_high_locality.jsonl", "comparison_high_locality.json"),
        ("low_locality", "trace_1000_low_locality.jsonl", "comparison_low_locality.json"),
    ];

    println!("pulzZ Collaborative-Doc Benchmark: Naive vs Predictive");
    println!("=======================================================");
    println!();

    let mut all_results = Vec::new();

    for (name, trace_file, result_file) in &traces {
        let trace_path = corpus_dir.join(trace_file);
        let result = run_baselines(name, trace_path.to_str().unwrap());

        println!("Trace: {name} ({trace_file})");
        println!("  Edits:       {}", result.n_edits);
        println!("  Edit types:  {:?}", result.edit_type_counts);
        println!("  Naive:       {} bytes ({:.1} bytes/edit)",
                 result.naive_total_bytes, result.naive_avg_bytes_per_edit);
        println!("  Predictive:  {} bytes ({:.1} bytes/edit)",
                 result.predictive_total_bytes, result.predictive_avg_bytes_per_edit);
        println!("  Reduction:   {:.1}%", result.reduction_pct);
        println!();

        // Write per-trace result JSON
        let result_path = results_dir.join(result_file);
        fs::write(&result_path, serde_json::to_string_pretty(&result).unwrap())
            .unwrap_or_else(|e| panic!("failed to write {result_path:?}: {e}"));

        all_results.push(result);
    }

    // Write combined result
    let combined_path = results_dir.join("comparison_all.json");
    fs::write(&combined_path, serde_json::to_string_pretty(&all_results).unwrap())
        .unwrap_or_else(|e| panic!("failed to write {combined_path:?}: {e}"));

    println!("Results written to: {}", results_dir.display());

    // DoD check
    println!();
    println!("DoD Check:");
    for r in &all_results {
        let status = if r.trace == "high_locality" {
            if r.reduction_pct >= 10.0 { "PASS" } else { "FAIL" }
        } else if r.trace == "low_locality" {
            if r.reduction_pct >= 0.0 { "PASS" } else { "FAIL" }
        } else {
            "INFO"
        };
        println!("  {}: {} — {:.1}% reduction ({} naive, {} predictive)",
                 r.trace, status, r.reduction_pct,
                 r.naive_total_bytes, r.predictive_total_bytes);
    }
}
