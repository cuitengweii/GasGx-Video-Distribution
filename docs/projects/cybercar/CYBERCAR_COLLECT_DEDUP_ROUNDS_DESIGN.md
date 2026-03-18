# CyberCar Collect Dedup And Round Rules Design

Last updated: 2026-03-18

Status: discussion-only / not implemented yet

## Purpose

Define a collection rule set that makes every new collect round prefer unseen X candidates, treats manual review and publish outcomes as future collect filters, and keeps the operator feed changing instead of resurfacing the same links repeatedly.

## Current Problem

- `history.txt` and `history_images.txt` already record some X status IDs, but they are not the single source of truth for future candidate filtering.
- `review_state.json` records manual outcomes, but it is keyed by processed file name instead of a stable candidate identity.
- Uploaded fingerprint indexes and uploaded records can block duplicate publish work, but they do not fully prevent the same X source from re-entering later collect rounds.
- The current collect limit behaves closer to "try N discovered items" than "deliver N unseen review candidates".

## Goals

- Each collect run targets `N` unseen review candidates, not merely `N` discovered URLs.
- Any candidate already reviewed, skipped, approved, or published should be filtered before download in later runs.
- The system should expand discovery rounds automatically when filters reduce the usable candidate count.
- Dedup should be source-aware first and fingerprint-aware second.

## Non-Goals

- This design does not change platform publish heuristics.
- This design does not replace content fingerprint dedup for near-duplicate media.
- This design does not require a new database; JSON-backed runtime state is acceptable for the first implementation.

## Canonical Candidate Identity

Every X candidate must be normalized to a stable `candidate_id` before download or review.

Recommended format:

```text
x:{status_id}:{media_key}
```

Rules:

- `status_id` is the X post/status ID.
- `media_key` is `media_id` when available.
- If a post exposes multiple images without stable media IDs, use a deterministic page key such as `p1`, `p2`, and so on.
- File names are never the primary identity.

Examples:

```text
x:2033523655636811999:2033523655636811999
x:2033523655636811999:p1
x:2033125031476162776:2033125031476162776
```

## Candidate Ledger

Add a unified runtime ledger:

```text
runtime/candidate_ledger.json
```

Suggested shape:

```json
{
  "version": 1,
  "updated_at": "2026-03-18 21:00:00",
  "items": {
    "x:2033125031476162776:2033125031476162776": {
      "source": "x",
      "candidate_id": "x:2033125031476162776:2033125031476162776",
      "status_id": "2033125031476162776",
      "media_key": "2033125031476162776",
      "media_kind": "video",
      "status_url": "https://x.com/...",
      "state": "published",
      "discovered_at": "2026-03-15 20:40:00",
      "last_seen_at": "2026-03-15 20:52:44",
      "cooldown_until": "",
      "retry_count": 0,
      "processed_name": "DRAFT_....mp4",
      "fingerprint": "132:...",
      "review": {
        "status": "approved",
        "updated_at": "2026-03-15 20:49:53"
      },
      "publish": {
        "status": "published",
        "updated_at": "2026-03-15 20:52:44"
      }
    }
  }
}
```

## State Model

Active states:

- `new_discovered`
- `downloaded`
- `processed`
- `review_pending`
- `download_failed_transient`
- `process_failed_transient`

Terminal states:

- `review_skipped`
- `rejected`
- `approved`
- `published`
- `publish_failed_terminal`
- `failed_terminal`

Filtering rule for future collect:

- Hard-filter terminal states immediately.
- Allow retry only for transient failure states that have passed cooldown.

## Review And Publish Closed Loop

Manual operator outcomes must become upstream collect filters.

Rules:

- Telegram skip/downvote writes `review_skipped` or `rejected`.
- Telegram approve writes `approved`.
- Publish success writes `published`.
- A confirmed non-recoverable publish failure may write `publish_failed_terminal`.
- Once a candidate reaches any terminal state above, later collect runs must not enqueue it again.

This is the main efficiency rule: manual effort should shrink the future candidate pool.

## Collect Target Semantics

`--limit` should mean:

```text
target_new_candidates
```

It should not mean:

```text
max_discovered_urls
```

If the operator asks for `--limit 5`, the pipeline should try to return five unseen review candidates after filtering.

## Discovery Round Rules

The collect cycle should oversample and expand in rounds.

Round 1:

- Discover budget: `max(target_new * 4, 24)`
- Normal seed accounts
- Current keyword set
- Normal scroll depth

Round 2:

- Trigger only if usable unseen candidates are still below target
- Discover budget: `max(target_new * 6, 36)`
- Add deeper scroll or more seed accounts
- Keep the same keyword family

Round 3:

- Trigger only if still below target
- Discover budget: `max(target_new * 8, 48)`
- Expand search breadth: additional seed accounts, broader query variants, or longer scroll depth

Stop conditions:

- Stop as soon as `usable_unseen >= target_new`
- Stop after round 3 even if target is not met
- Log the final reason: `target_met`, `round_cap_reached`, or `source_exhausted`

## Per-Round Filtering Order

The order must be cheap-first, expensive-later.

1. Discover candidate URLs and metadata.
2. Normalize each item to `candidate_id`.
3. Check `candidate_ledger.json`.
4. Drop candidates in terminal states.
5. Drop transient-failure candidates still inside cooldown.
6. Download only the remaining unseen candidates.
7. Process them and promote them to `review_pending`.

This prevents repeated download and processing of already-reviewed links.

## Retry And Cooldown Rules

Only transient technical failures may re-enter later rounds.

Suggested defaults:

- first transient failure: cooldown `6h`
- second transient failure: cooldown `24h`
- third transient failure: convert to `failed_terminal`

Rationale:

- network noise on X is real, so one failure should not poison the candidate forever
- repeated failure should not keep wasting future rounds

## Interaction With Fingerprint Dedup

Source dedup happens first. Fingerprint dedup stays as a second safety layer.

Rules:

- If `candidate_id` is already terminal, skip immediately even if fingerprint is missing.
- If `candidate_id` is unseen but its media fingerprint matches an already-published item within the configured threshold, skip it as a near-duplicate and write `failed_terminal` or `review_skipped` with a duplicate reason.
- Candidate dedup answers "have we already handled this source item?"
- Fingerprint dedup answers "is this media materially the same as something already published?"

## Migration Plan

Initial backfill should merge existing runtime evidence into the new ledger.

Sources:

- `runtime/history.txt`
- `runtime/history_images.txt`
- `runtime/review_state.json`
- `runtime/uploaded_content_fingerprint_index*.json`
- `runtime/uploaded_records_*.jsonl`

Backfill priority:

1. Recover `status_id` and `status_url`
2. Recover `processed_name`
3. Recover review outcome
4. Recover publish outcome
5. Recover fingerprint if available

If exact candidate identity cannot be reconstructed, preserve the old record as a weak legacy entry and do not drop it silently.

## Integration Points

Likely code touchpoints:

- `src/cybercar/engine.py`
  - X discovery normalization
  - pre-download candidate filtering
  - post-process ledger writes
- `src/cybercar/telegram/`
  - translate operator skip/approve actions into ledger state updates
- runtime state helpers
  - a new support module should own ledger load/save/merge logic instead of scattering JSON writes across the pipeline

## Acceptance Criteria

- A candidate skipped in Telegram does not reappear in later collect rounds.
- A candidate approved and published does not reappear in later collect rounds.
- `collect --limit 5` tries to deliver five unseen candidates, not merely five discovered URLs.
- When filtering removes too many candidates, the logs clearly show the next discovery round and the reason it was opened.
- Logs show a final summary such as:

```text
[Collect] target_new=5, delivered=5, rounds=2, filtered_seen=17, filtered_cooldown=3, filtered_duplicate_media=2
```

## Recommended Implementation Order

1. Introduce `candidate_ledger.json` and stable `candidate_id` normalization.
2. Backfill legacy runtime evidence into the ledger on load.
3. Apply ledger filtering before download.
4. Update Telegram review actions and publish results to write terminal ledger states.
5. Change collect semantics from "discover N" to "deliver N unseen" with round expansion.

## Operator Impact

After this design is implemented, every manual action becomes cumulative:

- skip once -> do not see it again
- reject once -> do not see it again
- approve and publish -> do not see it again
- transient X/network errors -> retry later under controlled cooldown

That gives the operator a moving feed instead of re-triaging old material.
