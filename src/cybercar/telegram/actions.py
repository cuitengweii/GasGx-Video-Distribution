from __future__ import annotations

from pathlib import Path

from . import legacy_worker


def run_home_action_job(**kwargs: object) -> int:
    normalized = dict(kwargs)
    if "repo_root" in normalized:
        normalized["repo_root"] = Path(str(normalized["repo_root"]))
    if "workspace" in normalized:
        normalized["workspace"] = Path(str(normalized["workspace"]))
    return legacy_worker._run_home_action_job(**normalized)


def run_comment_reply_job(**kwargs: object) -> int:
    normalized = dict(kwargs)
    if "repo_root" in normalized:
        normalized["repo_root"] = Path(str(normalized["repo_root"]))
    if "workspace" in normalized:
        normalized["workspace"] = Path(str(normalized["workspace"]))
    return legacy_worker._run_comment_reply_job(**normalized)


def run_collect_publish_latest_job(**kwargs: object) -> int:
    normalized = dict(kwargs)
    if "repo_root" in normalized:
        normalized["repo_root"] = Path(str(normalized["repo_root"]))
    if "workspace" in normalized:
        normalized["workspace"] = Path(str(normalized["workspace"]))
    return legacy_worker._run_collect_publish_latest_job(**normalized)


def run_immediate_publish_item_job(**kwargs: object) -> int:
    normalized = dict(kwargs)
    if "repo_root" in normalized:
        normalized["repo_root"] = Path(str(normalized["repo_root"]))
    if "workspace" in normalized:
        normalized["workspace"] = Path(str(normalized["workspace"]))
    return legacy_worker._run_immediate_publish_item_job(**normalized)


def run_immediate_collect_item_job(**kwargs: object) -> int:
    normalized = dict(kwargs)
    if "repo_root" in normalized:
        normalized["repo_root"] = Path(str(normalized["repo_root"]))
    if "workspace" in normalized:
        normalized["workspace"] = Path(str(normalized["workspace"]))
    return legacy_worker._run_immediate_collect_item_job(**normalized)
