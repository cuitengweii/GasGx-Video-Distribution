from __future__ import annotations

import argparse
import json
import re
import sys
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import unquote, urlparse
from urllib.request import Request as UrlRequest
from urllib.request import urlopen


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_URL_LIST = ROOT / "config" / "video_matrix" / "bgm_cloud_urls.txt"
DEFAULT_OUT_DIR = ROOT / "runtime" / "video_matrix" / "bgm_cloud"
DEFAULT_MANIFEST_NAME = "bgm_cloud_manifest.json"
DEFAULT_LIMIT = 300
MAX_LIMIT = 300
DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_MAX_BYTES = 80 * 1024 * 1024
DEFAULT_USER_AGENT = "GasGx Video Distribution BGM Cloud Importer/0.1"
CHUNK_SIZE = 64 * 1024


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_url_list(path: Path) -> list[str]:
    urls: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        value = line.strip()
        if not value or value.startswith("#"):
            continue
        urls.append(value)
    return urls


def _safe_mp3_filename(url: str, index: int) -> str:
    parsed = urlparse(url)
    raw_name = unquote(Path(parsed.path).name).strip()
    stem = Path(raw_name).stem if raw_name else f"bgm-{index:03d}"
    safe_stem = re.sub(r"[^A-Za-z0-9._-]+", "_", stem).strip("._-") or f"bgm-{index:03d}"
    return f"{safe_stem}.mp3"


def _is_valid_http_url(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _is_audio_mpeg(content_type: str) -> bool:
    return content_type.split(";", 1)[0].strip().lower() == "audio/mpeg"


def _response_can_be_mp3(url: str, content_type: str) -> bool:
    media_type = content_type.split(";", 1)[0].strip().lower()
    if _is_audio_mpeg(content_type):
        return True
    if media_type and media_type not in {"application/octet-stream", "binary/octet-stream"}:
        return False
    return urlparse(url).path.lower().endswith(".mp3")


def _load_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"version": 1, "entries": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Manifest is not valid JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"Manifest must contain a JSON object: {path}")
    entries = payload.get("entries")
    if not isinstance(entries, list):
        payload["entries"] = []
    payload["version"] = 1
    return payload


def _manifest_records(manifest: dict[str, Any]) -> OrderedDict[str, dict[str, Any]]:
    records: OrderedDict[str, dict[str, Any]] = OrderedDict()
    for entry in manifest.get("entries", []):
        if not isinstance(entry, dict):
            continue
        url = str(entry.get("url") or "").strip()
        if not url:
            continue
        records[url] = dict(entry)
    return records


def _entry_file_exists(entry: dict[str, Any], out_dir: Path) -> bool:
    filename = str(entry.get("filename") or "").strip()
    if not filename:
        return False
    return (out_dir / Path(filename).name).is_file()


def _successful_filenames(records: OrderedDict[str, dict[str, Any]], out_dir: Path) -> set[str]:
    filenames: set[str] = set()
    for entry in records.values():
        if entry.get("status") != "success" or not _entry_file_exists(entry, out_dir):
            continue
        filenames.add(Path(str(entry.get("filename"))).name)
    return filenames


def _read_response_limited(response: Any, max_bytes: int) -> bytes:
    content_length = ""
    headers = getattr(response, "headers", None)
    if headers is not None and hasattr(headers, "get"):
        content_length = str(headers.get("Content-Length") or "")
    if content_length:
        try:
            if int(content_length) > max_bytes:
                raise ValueError(f"audio file exceeds {max_bytes} bytes")
        except ValueError as exc:
            if "exceeds" in str(exc):
                raise

    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = response.read(CHUNK_SIZE)
        if not chunk:
            break
        total += len(chunk)
        if total > max_bytes:
            raise ValueError(f"audio file exceeds {max_bytes} bytes")
        chunks.append(chunk)
    return b"".join(chunks)


def _download_mp3(url: str, target: Path, timeout: float, max_bytes: int, user_agent: str) -> dict[str, Any]:
    request = UrlRequest(url, headers={"User-Agent": user_agent, "Accept": "audio/mpeg,*/*;q=0.8"})
    with urlopen(request, timeout=timeout) as response:
        http_status = int(getattr(response, "status", 200) or 200)
        headers = getattr(response, "headers", {})
        content_type = headers.get("Content-Type", "") if hasattr(headers, "get") else ""
        if not _response_can_be_mp3(url, content_type):
            raise ValueError(f"URL did not return an MP3 response: {content_type or 'missing Content-Type'}")
        body = _read_response_limited(response, max_bytes)
    target.parent.mkdir(parents=True, exist_ok=True)
    temp_target = target.with_name(f"{target.name}.part")
    temp_target.write_bytes(body)
    temp_target.replace(target)
    return {"http_status": http_status, "content_type": content_type, "size": len(body)}


def _entry(
    *,
    url: str,
    filename: str,
    out_dir: Path,
    status: str,
    reason: str,
    http_status: int | None = None,
    content_type: str = "",
    size: int | None = None,
) -> dict[str, Any]:
    target = out_dir / filename if filename else out_dir
    payload: dict[str, Any] = {
        "url": url,
        "filename": filename,
        "path": str(target.resolve()) if filename else "",
        "status": status,
        "reason": reason,
        "http_status": http_status,
        "content_type": content_type,
        "recorded_at": utc_now(),
    }
    if status == "success":
        payload["downloaded_at"] = payload["recorded_at"]
    if size is not None:
        payload["size"] = size
    return payload


def _build_manifest(
    records: OrderedDict[str, dict[str, Any]],
    out_dir: Path,
    limit: int,
    run_summary: dict[str, int] | None = None,
) -> dict[str, Any]:
    entries = list(records.values())
    success_entries = [entry for entry in entries if entry.get("status") == "success" and _entry_file_exists(entry, out_dir)]
    failed_entries = [entry for entry in entries if entry.get("status") == "failed"]
    skipped_entries = [entry for entry in entries if entry.get("status") == "skipped"]
    manifest = {
        "version": 1,
        "updated_at": utc_now(),
        "out_dir": str(out_dir.resolve()),
        "limit": limit,
        "entries": entries,
        "summary": {
            "success_count": len(success_entries),
            "failed_count": len(failed_entries),
            "skipped_count": len(skipped_entries),
            "file_count": len(list(out_dir.glob("*.mp3"))) if out_dir.exists() else 0,
            "limit": limit,
        },
    }
    if run_summary is not None:
        manifest["summary"]["run"] = dict(run_summary)
    return manifest


def _save_manifest(path: Path, manifest: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.tmp")
    temp_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temp_path.replace(path)


def _normalize_limit(limit: int) -> int:
    if limit < 1:
        raise ValueError("limit must be at least 1")
    return min(limit, MAX_LIMIT)


def import_bgm_cloud(
    *,
    urls_path: Path,
    out_dir: Path,
    manifest_path: Path | None = None,
    limit: int = DEFAULT_LIMIT,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    max_bytes: int = DEFAULT_MAX_BYTES,
    user_agent: str = DEFAULT_USER_AGENT,
) -> dict[str, Any]:
    limit = _normalize_limit(limit)
    if not urls_path.exists():
        raise FileNotFoundError(f"URL list does not exist: {urls_path}")
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_path or out_dir / DEFAULT_MANIFEST_NAME
    urls = read_url_list(urls_path)
    records = _manifest_records(_load_manifest(manifest_path))
    successful_filenames = _successful_filenames(records, out_dir)
    successful_urls = {
        url for url, entry in records.items()
        if entry.get("status") == "success" and _entry_file_exists(entry, out_dir)
    }
    run_summary = {
        "downloaded": 0,
        "existing": 0,
        "resumed": 0,
        "failed": 0,
        "skipped": 0,
        "duplicate_url": 0,
    }
    seen_urls: set[str] = set()

    def persist() -> dict[str, Any]:
        manifest = _build_manifest(records, out_dir, limit, run_summary)
        _save_manifest(manifest_path, manifest)
        return manifest

    manifest = persist()
    for index, url in enumerate(urls, start=1):
        if url in seen_urls:
            run_summary["duplicate_url"] += 1
            run_summary["skipped"] += 1
            manifest = persist()
            continue
        seen_urls.add(url)
        filename = _safe_mp3_filename(url, index)
        target = out_dir / filename

        if url in successful_urls:
            run_summary["resumed"] += 1
            manifest = persist()
            continue
        if len(successful_filenames) >= limit:
            break
        if not _is_valid_http_url(url):
            records[url] = _entry(url=url, filename=filename, out_dir=out_dir, status="failed", reason="invalid_url")
            run_summary["failed"] += 1
            manifest = persist()
            continue
        if filename in successful_filenames:
            records[url] = _entry(
                url=url,
                filename=filename,
                out_dir=out_dir,
                status="skipped",
                reason="duplicate_filename",
            )
            run_summary["skipped"] += 1
            manifest = persist()
            continue
        if target.exists():
            records[url] = _entry(
                url=url,
                filename=filename,
                out_dir=out_dir,
                status="success",
                reason="existing_file",
                size=target.stat().st_size,
            )
            successful_urls.add(url)
            successful_filenames.add(filename)
            run_summary["existing"] += 1
            manifest = persist()
            continue

        try:
            download = _download_mp3(url, target, timeout, max_bytes, user_agent)
        except HTTPError as exc:
            records[url] = _entry(
                url=url,
                filename=filename,
                out_dir=out_dir,
                status="failed",
                reason=str(exc.reason or exc),
                http_status=int(exc.code),
            )
            run_summary["failed"] += 1
            manifest = persist()
            continue
        except Exception as exc:
            records[url] = _entry(
                url=url,
                filename=filename,
                out_dir=out_dir,
                status="failed",
                reason=str(exc),
            )
            run_summary["failed"] += 1
            manifest = persist()
            continue

        records[url] = _entry(
            url=url,
            filename=filename,
            out_dir=out_dir,
            status="success",
            reason="downloaded",
            http_status=download["http_status"],
            content_type=download["content_type"],
            size=download["size"],
        )
        successful_urls.add(url)
        successful_filenames.add(filename)
        run_summary["downloaded"] += 1
        manifest = persist()

    return manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Import authorized MP3 URLs into the Video Matrix bgm_cloud folder.")
    parser.add_argument("--urls", type=Path, default=DEFAULT_URL_LIST, help="Path to a newline-delimited URL list.")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT_DIR, help="Destination directory for imported MP3 files.")
    parser.add_argument("--manifest", type=Path, default=None, help="Manifest path. Defaults to bgm_cloud_manifest.json in --out.")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help=f"Maximum successful imports, capped at {MAX_LIMIT}.")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS, help="Per-request timeout in seconds.")
    parser.add_argument("--max-mb", type=float, default=DEFAULT_MAX_BYTES / (1024 * 1024), help="Maximum MP3 size in MB.")
    parser.add_argument(
        "--allow-content-id",
        action="store_true",
        help="Accepted for the approved workflow; URL lists are imported without Content ID filtering.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        manifest = import_bgm_cloud(
            urls_path=args.urls,
            out_dir=args.out,
            manifest_path=args.manifest,
            limit=args.limit,
            timeout=args.timeout,
            max_bytes=int(args.max_mb * 1024 * 1024),
        )
    except Exception as exc:
        print(f"import_bgm_cloud: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(manifest["summary"], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
