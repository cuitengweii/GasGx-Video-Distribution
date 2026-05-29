# CyberCar Run Notes - 2026-05-29

## Latest `#1` Account Xiaohongshu Publish

- Run workspace: `runtime/account_publish_runs/20260529_122418_636336_a-01_domestic/xiaohongshu`
- Asset: `vibe_02.mp4`
- Start: `2026-05-29 12:24:19`
- Finish: `2026-05-29 12:27:44`
- Result: failed
- Return code: `0`
- Evidence: `false`
- Error: `xiaohongshu publish returned 0 but no uploaded_records_xiaohongshu.jsonl evidence was written`

## What Went Wrong

- The run exited cleanly, but the platform-specific evidence file was never written.
- The publish wrapper therefore classified the attempt as unsuccessful even though the subprocess return code was `0`.
- `matrix_xiaohongshu_publish.log` in that workspace is empty, so there is no deeper step-by-step trace from that run.

## Operator Takeaway

- For Xiaohongshu publishes, a zero exit code is not enough.
- Success must be treated as "returned cleanly plus evidence file written".
- If the log is empty and `uploaded_records_xiaohongshu.jsonl` is missing, the run should be treated as a process failure, not a successful publish.
