use std::{env, path::PathBuf, process};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("server command failed: {error}");
        process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    match args.next().as_deref() {
        Some("bench") => {
            ensure_release_benchmark_build()?;
            let subcommand = args.next().unwrap_or_else(|| "help".to_string());
            match subcommand.as_str() {
                "run" => {
                    let remaining = args.collect::<Vec<_>>();
                    if remaining.len() < 6 {
                        return Err("bench run expects <environment> <chpmt_capability> <workload> <protection> <mode> <size> [carrier] [runtime] [direction] [corpus] [optimization] [artifact_root]".into());
                    }
                    let mut case_arg_len = 6;
                    while let Some(value) = remaining.get(case_arg_len) {
                        let is_optional_case_arg = server::bench::BenchmarkCarrier::parse(value)
                            .is_some()
                            || server::bench::BenchmarkClientRuntime::parse(value).is_some()
                            || server::bench::BenchmarkDirection::parse(value).is_some()
                            || server::bench::BenchmarkCorpusKind::parse(value).is_some()
                            || server::bench::BenchmarkOptimization::parse(value).is_some();
                        if !is_optional_case_arg {
                            break;
                        }
                        case_arg_len += 1;
                    }
                    let case = server::bench::parse_case_from_args(&remaining[..case_arg_len])?;
                    let artifact_root = remaining
                        .get(case_arg_len)
                        .map(PathBuf::from)
                        .unwrap_or_else(|| PathBuf::from("benchmarks/tmp"));
                    if remaining.len() > case_arg_len + 1 {
                        return Err("bench run received too many trailing arguments".into());
                    }
                    let result = server::bench::run_benchmark_case(case, &artifact_root).await?;
                    println!(
                        "benchmark complete: {} -> {}",
                        result.case.display_name(),
                        result.artifact_dir
                    );
                    Ok(())
                }
                "fetch-corpus" => {
                    let corpus = args
                        .next()
                        .and_then(|value| server::bench::BenchmarkCorpusKind::parse(&value))
                        .ok_or("bench fetch-corpus expects <wikitext_103_raw|web_image_50>")?;
                    server::bench::fetch_benchmark_corpus(corpus)?;
                    match corpus {
                        server::bench::BenchmarkCorpusKind::Wikitext103Raw => {
                            println!("benchmark corpus is streamed at runtime: {}", corpus.slug());
                        }
                        server::bench::BenchmarkCorpusKind::WebImage50 => {
                            println!("benchmark corpus materialized: {}", corpus.slug());
                        }
                    }
                    Ok(())
                }
                "matrix" => {
                    let remaining = args.collect::<Vec<_>>();
                    let (cases, artifact_root) = match remaining.first().map(String::as_str) {
                        Some("smoke") => (
                            server::bench::smoke_matrix(),
                            remaining
                                .get(1)
                                .map(PathBuf::from)
                                .unwrap_or_else(|| PathBuf::from("benchmarks/smoke")),
                        ),
                        Some("production_remote_bidirectional") => (
                            server::bench::production_remote_bidirectional_matrix(),
                            remaining.get(1).map(PathBuf::from).unwrap_or_else(|| {
                                PathBuf::from(server::bench::DEFAULT_PRODUCTION_BENCH_ARTIFACT_ROOT)
                            }),
                        ),
                        Some("production_remote_bidirectional_web_image_50") => (
                            server::bench::production_remote_bidirectional_web_image_matrix(),
                            remaining.get(1).map(PathBuf::from).unwrap_or_else(|| {
                                PathBuf::from("benchmarks/mutual_pqc_web_image_50")
                            }),
                        ),
                        Some("websocket_capabilities") => (
                            server::bench::websocket_capability_comparison_matrix(),
                            remaining.get(1).map(PathBuf::from).unwrap_or_else(|| {
                                PathBuf::from("benchmarks/websocket_capabilities")
                            }),
                        ),
                        Some("reliable_carriers") => (
                            server::bench::reliable_carrier_comparison_matrix(),
                            remaining
                                .get(1)
                                .map(PathBuf::from)
                                .unwrap_or_else(|| PathBuf::from("benchmarks/reliable_carriers")),
                        ),
                        Some("datagram_native_carriers") => (
                            server::bench::datagram_native_carrier_matrix(),
                            remaining.get(1).map(PathBuf::from).unwrap_or_else(|| {
                                PathBuf::from("benchmarks/datagram_native_carriers")
                            }),
                        ),
                        Some("datagram_native_smoke") => (
                            server::bench::datagram_native_smoke_matrix(),
                            remaining.get(1).map(PathBuf::from).unwrap_or_else(|| {
                                PathBuf::from("benchmarks/datagram_native_smoke")
                            }),
                        ),
                        Some("production_source_optimization_comparison") => (
                            server::bench::production_source_optimization_comparison_slice(),
                            remaining.get(1).map(PathBuf::from).unwrap_or_else(|| {
                                PathBuf::from(
                                    "benchmarks/production_source_optimization_comparison",
                                )
                            }),
                        ),
                        _ => (
                            server::bench::default_full_matrix(),
                            remaining
                                .first()
                                .map(PathBuf::from)
                                .unwrap_or_else(|| PathBuf::from("benchmarks/tmp")),
                        ),
                    };
                    let result = server::bench::run_benchmark_matrix(cases, &artifact_root).await?;
                    println!(
                        "benchmark matrix complete: {} succeeded, {} failed under {}",
                        result.completed_cases, result.failed_cases, result.artifact_root
                    );
                    Ok(())
                }
                "distributed-native-matrix" => {
                    let remaining = args.collect::<Vec<_>>();
                    if remaining.len() < 4 {
                        return Err("bench distributed-native-matrix expects <reliable_carriers|datagram_native_carriers|distributed_native_all|websocket_mutual_all_sizes> <client_ssh_target> <client_repo_path> <server_host> [artifact_root]".into());
                    }
                    let matrix_name = &remaining[0];
                    let cases = server::bench::named_matrix(matrix_name).ok_or_else(|| {
                        format!("unknown distributed native matrix `{matrix_name}`")
                    })?;
                    let client_ssh_target = &remaining[1];
                    let client_repo_path = &remaining[2];
                    let server_host = &remaining[3];
                    let artifact_root = remaining
                        .get(4)
                        .map(PathBuf::from)
                        .unwrap_or_else(|| PathBuf::from(format!("benchmarks/{matrix_name}")));
                    let result = server::bench::run_distributed_native_matrix(
                        cases,
                        &artifact_root,
                        client_ssh_target,
                        client_repo_path,
                        server_host,
                    )
                    .await?;
                    println!(
                        "distributed native benchmark matrix complete: {} succeeded, {} failed under {}",
                        result.completed_cases, result.failed_cases, result.artifact_root
                    );
                    Ok(())
                }
                "serve" => {
                    let manifest_path = args
                        .next()
                        .map(PathBuf::from)
                        .ok_or("bench serve expects <case_json> <artifact_dir>")?;
                    let artifact_dir = args
                        .next()
                        .map(PathBuf::from)
                        .ok_or("bench serve expects <case_json> <artifact_dir>")?;
                    let metrics =
                        server::bench::serve_case_from_manifest(&manifest_path, &artifact_dir)
                            .await?;
                    println!(
                        "benchmark serve complete: {} records written under {}",
                        metrics.records,
                        artifact_dir.display()
                    );
                    Ok(())
                }
                "verify" => {
                    let manifest_path = args
                        .next()
                        .map(PathBuf::from)
                        .ok_or("bench verify expects <case_json> <url> <artifact_dir>")?;
                    let url = args
                        .next()
                        .ok_or("bench verify expects <case_json> <url> <artifact_dir>")?;
                    let artifact_dir = args
                        .next()
                        .map(PathBuf::from)
                        .ok_or("bench verify expects <case_json> <url> <artifact_dir>")?;
                    let metrics = server::bench::verify_case_from_manifest(
                        &manifest_path,
                        &url,
                        &artifact_dir,
                    )
                    .await?;
                    println!(
                        "benchmark verify complete: {} records written under {}",
                        metrics.records,
                        artifact_dir.display()
                    );
                    Ok(())
                }
                "help" | "--help" | "-h" => {
                    print_help();
                    Ok(())
                }
                other => Err(format!("unknown bench subcommand: {other}").into()),
            }
        }
        Some("eval") => {
            let output_dir = args
                .next()
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("artifacts/eval"));
            if args.next().is_some() {
                return Err("eval expects at most one optional <output_dir> argument".into());
            }
            let report = server::eval::run_default_evaluation(&output_dir)?;
            println!(
                "evaluation complete (chpmt_default): {} workload reports written under {}",
                report.workloads.len(),
                output_dir.display()
            );
            Ok(())
        }
        Some("serve") => {
            let scenario = args
                .next()
                .as_deref()
                .and_then(server::scenario::ScenarioKind::parse)
                .unwrap_or(server::scenario::ScenarioKind::Full);
            let addr = args
                .next()
                .unwrap_or_else(|| server::scenario::DEFAULT_SCENARIO_ADDR.to_string());
            let connections = args
                .next()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(1);
            server::scenario::serve_at(scenario, &addr, connections).await?;
            println!(
                "served scenario `{}` on {} for {} connection(s)",
                scenario.slug(),
                addr,
                connections
            );
            Ok(())
        }
        Some("verify") => {
            let scenario = args
                .next()
                .as_deref()
                .and_then(server::scenario::ScenarioKind::parse)
                .unwrap_or(server::scenario::ScenarioKind::Full);
            let url = args
                .next()
                .unwrap_or_else(|| server::scenario::DEFAULT_SCENARIO_URL.to_string());
            let repetitions = args
                .next()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(1);
            let output_path = args.next().map(PathBuf::from).unwrap_or_else(|| {
                PathBuf::from(format!(
                    "artifacts/distributed/{}_report.json",
                    scenario.slug()
                ))
            });
            let report = server::scenario::verify_url(scenario, &url, repetitions).await?;
            server::scenario::write_report(&report, &output_path)?;
            println!("{}", server::scenario::render_report(&report));
            println!("report written to {}", output_path.display());
            Ok(())
        }
        Some("help") | Some("--help") | Some("-h") => {
            print_help();
            Ok(())
        }
        None => {
            print_help();
            Ok(())
        }
        Some(other) => Err(format!("unknown server command: {other}").into()),
    }
}

fn print_help() {
    println!(
        "pulzz server commands:
  cargo run -p server -- eval [output_dir]
  cargo run -p server -- serve [full] [addr] [connections]
  cargo run -p server -- verify [full] [url] [repetitions] [output_json]
  cargo run --release -p server -- bench fetch-corpus <wikitext_103_raw|web_image_50>
  cargo run --release -p server -- bench run <environment> <chpmt_capability> <workload> <protection> <mode> <size> [carrier] [runtime] [direction] [corpus] [optimization] [artifact_root]
  cargo run --release -p server -- bench matrix [artifact_root]
  cargo run --release -p server -- bench matrix smoke [artifact_root]
  cargo run --release -p server -- bench matrix production_remote_bidirectional [artifact_root]
  cargo run --release -p server -- bench matrix production_remote_bidirectional_web_image_50 [artifact_root]
  cargo run --release -p server -- bench matrix websocket_capabilities [artifact_root]
  cargo run --release -p server -- bench matrix reliable_carriers [artifact_root]
  cargo run --release -p server -- bench matrix datagram_native_carriers [artifact_root]
  cargo run --release -p server -- bench matrix datagram_native_smoke [artifact_root]
  cargo run --release -p server -- bench matrix production_source_optimization_comparison [artifact_root]
  cargo run --release -p server -- bench distributed-native-matrix <reliable_carriers|datagram_native_carriers|distributed_native_all|websocket_mutual_all_sizes> <client_ssh_target> <client_repo_path> <server_host> [artifact_root]
  cargo run --release -p server -- bench serve <case_json> <artifact_dir>
  cargo run --release -p server -- bench verify <case_json> <url> <artifact_dir>
  capabilities: text_family_cue_object for text/json/binary corpora, image_family_cue_object for image corpora
  note: benchmark capability descriptors are derived from the active CHPMT architecture
  note: wikitext_103_raw is streamed at runtime; web_image_50 is materialized locally
  cargo run -p server -- help"
    );
}

fn ensure_release_benchmark_build() -> Result<(), Box<dyn std::error::Error>> {
    if cfg!(debug_assertions) {
        return Err(
            "benchmark commands must be run with --release so scale measurements are meaningful"
                .into(),
        );
    }
    Ok(())
}
