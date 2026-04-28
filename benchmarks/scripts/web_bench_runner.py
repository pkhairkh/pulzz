#!/usr/bin/env python3

import argparse
import json
import mimetypes
import os
import socket
import threading
import time
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import quote

from selenium import webdriver
from selenium.webdriver.chrome.options import Options


POLL_INTERVAL_SECONDS = 0.5


@dataclass
class WebBenchSample:
    elapsed_ms: int
    rss_bytes: int
    vsz_bytes: int
    cpu_percent: float
    records: int
    predictive_records: int
    payload_bytes: int
    wire_bytes: int


class DualRootHandler(BaseHTTPRequestHandler):
    bundle_root: Path = Path(".")
    case_root: Path = Path(".")

    def do_GET(self) -> None:
        relative_path = self.path.split("?", 1)[0]
        if relative_path in ("", "/"):
            relative_path = "/index.html"

        file_path = self.resolve_path(relative_path)
        if file_path is None or not file_path.exists() or not file_path.is_file():
            self.send_error(HTTPStatus.NOT_FOUND, "not found")
            return

        content_type, _ = mimetypes.guess_type(str(file_path))
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type or "application/octet-stream")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        with file_path.open("rb") as handle:
            self.wfile.write(handle.read())

    def log_message(self, format: str, *args) -> None:
        return

    @classmethod
    def resolve_path(cls, relative_path: str) -> Path | None:
        if relative_path == "/index.html":
            return cls.bundle_root / "index.html"
        if relative_path == "/web_bench_case.json":
            return cls.case_root / "web_bench_case.json"
        if relative_path == "/web_bench_frames.bin":
            return cls.case_root / "web_bench_frames.bin"
        if relative_path.startswith("/pkg/"):
            remainder = relative_path.removeprefix("/pkg/")
            return cls.bundle_root / "pkg" / remainder
        return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a Chrome-backed pulzz web bench benchmark")
    parser.add_argument("--bundle-root", required=True)
    parser.add_argument("--case-root", required=True)
    parser.add_argument("--ws-url", required=True)
    parser.add_argument("--timeout-seconds", type=float, default=3600.0)
    return parser.parse_args()


def make_server(bundle_root: Path, case_root: Path) -> ThreadingHTTPServer:
    handler = type("BoundDualRootHandler", (DualRootHandler,), {})
    handler.bundle_root = bundle_root
    handler.case_root = case_root
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    return httpd


def start_server(httpd: ThreadingHTTPServer) -> threading.Thread:
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    return thread


def chrome_options() -> Options:
    os.environ["PATH"] = "/usr/bin:/bin:/usr/sbin:/sbin"
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-first-run")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--disable-background-networking")
    options.add_argument("--disable-dev-shm-usage")
    return options


def metrics_dict(driver: webdriver.Chrome) -> dict[str, float]:
    metrics = driver.execute_cdp_cmd("Performance.getMetrics", {})
    return {entry["name"]: entry["value"] for entry in metrics.get("metrics", [])}


def main() -> int:
    args = parse_args()
    bundle_root = Path(args.bundle_root).resolve()
    case_root = Path(args.case_root).resolve()
    httpd = make_server(bundle_root, case_root)
    start_server(httpd)
    host, port = httpd.server_address
    page_url = f"http://{host}:{port}/index.html?ws={quote(args.ws_url, safe=':/?&=%')}"

    driver = None
    try:
        driver = webdriver.Chrome(options=chrome_options())
        driver.execute_cdp_cmd("Performance.enable", {})
        driver.get(page_url)

        start = time.monotonic()
        last_sample_at = start
        last_task_duration = 0.0
        samples: list[WebBenchSample] = []
        while True:
            state = driver.execute_script(
                """
                return {
                  result: window.__pulzzRuntimeResult || null,
                  error: window.__pulzzRuntimeError || null,
                  status: window.__pulzzRuntimeStatus || null,
                  progress: window.__pulzzRuntimeProgress || null
                };
                """
            )
            now = time.monotonic()
            metrics = metrics_dict(driver)
            elapsed = now - start
            task_duration = float(metrics.get("TaskDuration", 0.0))
            delta_wall = max(now - last_sample_at, 1e-9)
            task_cpu_percent = max(
                0.0, ((task_duration - last_task_duration) / delta_wall) * 100.0
            )
            last_sample_at = now
            last_task_duration = task_duration

            progress = state.get("progress") or {}
            sample = WebBenchSample(
                elapsed_ms=int(progress.get("elapsed_ms", round(elapsed * 1000.0))),
                rss_bytes=int(metrics.get("JSHeapUsedSize", 0.0)),
                vsz_bytes=int(metrics.get("JSHeapTotalSize", 0.0)),
                cpu_percent=task_cpu_percent,
                records=int(progress.get("records", 0)),
                predictive_records=int(progress.get("predictive_records", progress.get("cue_object_records", progress.get("vector_records", 0)))),
                payload_bytes=int(progress.get("payload_bytes", 0)),
                wire_bytes=int(progress.get("wire_bytes", 0)),
            )
            samples.append(sample)

            if state.get("error"):
                raise RuntimeError(str(state["error"]))
            if state.get("result") is not None:
                peak_rss = max((entry.rss_bytes for entry in samples), default=0)
                peak_vsz = max((entry.vsz_bytes for entry in samples), default=0)
                peak_cpu = max((entry.cpu_percent for entry in samples), default=0.0)
                print(
                    json.dumps(
                        {
                            "result": state["result"],
                            "samples": [entry.__dict__ for entry in samples],
                            "peak_rss_bytes": peak_rss,
                            "peak_vsz_bytes": peak_vsz,
                            "peak_cpu_percent": peak_cpu,
                        }
                    )
                )
                return 0

            if elapsed > args.timeout_seconds:
                raise TimeoutError(
                    f"web bench benchmark timed out after {args.timeout_seconds} seconds"
                )
            time.sleep(POLL_INTERVAL_SECONDS)
    finally:
        httpd.shutdown()
        httpd.server_close()
        if driver is not None:
            driver.quit()


if __name__ == "__main__":
    raise SystemExit(main())
