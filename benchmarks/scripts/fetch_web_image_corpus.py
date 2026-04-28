#!/usr/bin/env python3

import hashlib
import json
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CORPUS_DIR = ROOT / "benchmarks" / "input_corpora" / "web_image_50"
FILES_DIR = CORPUS_DIR / "files"
MANIFEST_PATH = CORPUS_DIR / "manifest.json"
USER_AGENT = "pulzz-bench/1.0 (local benchmark corpus fetcher)"

JPEG_SOURCES = [
    ("jpeg_01", "https://picsum.photos/id/10/240/160.jpg"),
    ("jpeg_02", "https://picsum.photos/id/20/320/180.jpg"),
    ("jpeg_03", "https://picsum.photos/id/30/360/240.jpg"),
    ("jpeg_04", "https://picsum.photos/id/40/480/320.jpg"),
    ("jpeg_05", "https://picsum.photos/id/50/512/384.jpg"),
    ("jpeg_06", "https://picsum.photos/id/60/640/360.jpg"),
    ("jpeg_07", "https://picsum.photos/id/70/720/480.jpg"),
    ("jpeg_08", "https://picsum.photos/id/80/800/600.jpg"),
    ("jpeg_09", "https://picsum.photos/id/90/960/540.jpg"),
    ("jpeg_10", "https://picsum.photos/id/100/1024/768.jpg"),
]

PNG_SOURCES = [
    ("png_01", "https://dummyimage.com/240x160/ffcc00/000.png&text=PNG+01"),
    ("png_02", "https://dummyimage.com/300x180/0088cc/fff.png&text=PNG+02"),
    ("png_03", "https://dummyimage.com/360x240/2a9d8f/fff.png&text=PNG+03"),
    ("png_04", "https://dummyimage.com/420x240/e76f51/fff.png&text=PNG+04"),
    ("png_05", "https://dummyimage.com/480x320/264653/fff.png&text=PNG+05"),
    ("png_06", "https://dummyimage.com/540x360/a8dadc/111.png&text=PNG+06"),
    ("png_07", "https://dummyimage.com/640x360/8338ec/fff.png&text=PNG+07"),
    ("png_08", "https://dummyimage.com/720x480/f4a261/111.png&text=PNG+08"),
    ("png_09", "https://dummyimage.com/900x500/1d3557/fff.png&text=PNG+09"),
    ("png_10", "https://dummyimage.com/1024x768/457b9d/fff.png&text=PNG+10"),
]

GIF_SOURCES = [
    ("gif_01", "https://dummyimage.com/240x160/f94144/fff.gif&text=GIF+01"),
    ("gif_02", "https://dummyimage.com/300x180/f3722c/111.gif&text=GIF+02"),
    ("gif_03", "https://dummyimage.com/360x240/f8961e/111.gif&text=GIF+03"),
    ("gif_04", "https://dummyimage.com/420x240/f9844a/111.gif&text=GIF+04"),
    ("gif_05", "https://dummyimage.com/480x320/f9c74f/111.gif&text=GIF+05"),
    ("gif_06", "https://dummyimage.com/540x360/90be6d/111.gif&text=GIF+06"),
    ("gif_07", "https://dummyimage.com/640x360/43aa8b/fff.gif&text=GIF+07"),
    ("gif_08", "https://dummyimage.com/720x480/4d908e/fff.gif&text=GIF+08"),
    ("gif_09", "https://dummyimage.com/900x500/577590/fff.gif&text=GIF+09"),
    ("gif_10", "https://dummyimage.com/1024x768/277da1/fff.gif&text=GIF+10"),
]

WEBP_SOURCES = [
    ("webp_01", "https://www.gstatic.com/webp/gallery/1.webp"),
    ("webp_02", "https://www.gstatic.com/webp/gallery/2.webp"),
    ("webp_03", "https://www.gstatic.com/webp/gallery/3.webp"),
    ("webp_04", "https://www.gstatic.com/webp/gallery/4.webp"),
    ("webp_05", "https://www.gstatic.com/webp/gallery/5.webp"),
    ("webp_06", "https://www.gstatic.com/webp/gallery3/1_webp_ll.webp"),
    ("webp_07", "https://www.gstatic.com/webp/gallery3/2_webp_ll.webp"),
    ("webp_08", "https://www.gstatic.com/webp/gallery3/3_webp_ll.webp"),
    ("webp_09", "https://www.gstatic.com/webp/gallery3/4_webp_ll.webp"),
    ("webp_10", "https://www.gstatic.com/webp/gallery3/5_webp_ll.webp"),
]

SVG_NAMES = [
    "academic-cap",
    "adjustments-horizontal",
    "archive-box",
    "beaker",
    "bell-alert",
    "bolt",
    "bug-ant",
    "camera",
    "cloud",
    "cpu-chip",
]

SVG_SOURCES = [
    (
        f"svg_{index + 1:02d}",
        f"https://raw.githubusercontent.com/tailwindlabs/heroicons/master/src/24/outline/{name}.svg",
    )
    for index, name in enumerate(SVG_NAMES)
]

ALL_SOURCES = (
    [("image/jpeg", ".jpg", *item) for item in JPEG_SOURCES]
    + [("image/png", ".png", *item) for item in PNG_SOURCES]
    + [("image/gif", ".gif", *item) for item in GIF_SOURCES]
    + [("image/webp", ".webp", *item) for item in WEBP_SOURCES]
    + [("image/svg+xml", ".svg", *item) for item in SVG_SOURCES]
)


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def download(url: str) -> tuple[bytes, str]:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as response:
        return response.read(), response.headers.get_content_type()


def main() -> int:
    CORPUS_DIR.mkdir(parents=True, exist_ok=True)
    FILES_DIR.mkdir(parents=True, exist_ok=True)
    for path in FILES_DIR.iterdir():
        if path.is_file():
            path.unlink()

    manifest_files: list[dict] = []
    for mime, extension, slug, url in ALL_SOURCES:
        filename = f"{slug}{extension}"
        absolute_path = FILES_DIR / filename
        data, response_mime = download(url)
        absolute_path.write_bytes(data)
        manifest_files.append(
            {
                "relative_path": (Path("benchmarks") / "input_corpora" / "web_image_50" / "files" / filename).as_posix(),
                "source_url": url,
                "source_page_url": url,
                "mime": response_mime or mime,
                "byte_len": len(data),
                "sha256": sha256_hex(data),
            }
        )
        print(f"downloaded {filename} ({len(data)} bytes, {response_mime or mime})")
        time.sleep(0.15)

    manifest = {
        "source": "mixed_web_image_sources_v1",
        "file_count": len(manifest_files),
        "files": manifest_files,
    }
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, sort_keys=False) + "\n")
    print(f"wrote {MANIFEST_PATH} with {len(manifest_files)} files")
    return 0


if __name__ == "__main__":
    sys.exit(main())
