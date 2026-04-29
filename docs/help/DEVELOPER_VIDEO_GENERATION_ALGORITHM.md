# Developer Help: Video Generation Algorithm

This document records the current GasGx video-generation algorithm for developer lookup. It describes the implemented code path as of 2026-04-29; it is not a product or marketing explanation.

## Entry Points

- `G:\GasGx Video Distribution\src\gasgx_distribution\video_matrix\pipeline.py`
- `G:\GasGx Video Distribution\src\gasgx_distribution\video_matrix\ingestion.py`
- `G:\GasGx Video Distribution\src\gasgx_distribution\video_matrix\composition.py`
- `G:\GasGx Video Distribution\src\gasgx_distribution\video_matrix\render.py`
- `G:\GasGx Video Distribution\src\gasgx_distribution\video_matrix\beat.py`

## High-Level Algorithm

The current generator is a material-remix pipeline. It does not synthesize video frames from a text prompt. It ingests existing clips, normalizes them, buckets them by category, cuts beat-aligned segments, creates de-duplicated variants, and renders the final vertical videos with FFmpeg overlays and BGM.

## Pipeline Steps

1. Source ingestion
   - Reads video files from the configured `source_root`, usually `runtime/video_matrix/incoming`.
   - Supports `.mp4`, `.mov`, `.m4v`, `.avi`, and `.mkv`.
   - Infers material categories from path parts and filename keywords.
   - Writes normalized clips under the configured `library_root`.

2. Clip normalization
   - Uses FFmpeg to convert each clip to the configured target size and frame rate.
   - Current defaults are `1080x1920` and `60fps`.
   - Removes source audio.
   - Applies light image tuning through `eq=contrast=1.15:brightness=-0.03:saturation=1.05`.

3. HUD data preparation
   - Builds HUD lines from live data when available.
   - Live mode tries BTC/USD and network hashrate endpoints.
   - If live data fails, the pipeline falls back to `hud_fixed_formulas` from `config/video_matrix/defaults.json`.

4. BGM beat grid
   - Uses `librosa` to detect beats from the selected BGM.
   - If `librosa` is missing or beat detection fails, it falls back to a fixed grid with about `0.48s` spacing.
   - Segment durations are aligned against this beat grid.

5. Variant planning
   - Uses `composition_sequence` from `config/video_matrix/defaults.json` or the generate request.
   - The default sequence preserves the previous behavior:

```text
category_A -> category_B -> category_A -> category_C
```

   - The default target segment windows preserve the previous behavior:

```text
1.5s -> 3.4s -> 1.5s -> 3.0s
```

   - Each row can now be changed in the video-generation UI by selecting a category and segment duration.
   - For each segment, randomly chooses a clip from the required category and randomly chooses a valid start time.
   - Randomly chooses title, slogan, LUT strength, zoom, mirror flag, and crop offsets.

6. De-duplication
   - Builds a SHA1 signature from segment clip IDs, start times, durations, title, slogan, visual parameters, and HUD lines.
   - If a signature has already been used in the batch, planning retries.
   - If `variant_history_enabled` is true, previously generated signatures are loaded from `runtime/video_matrix/signature_history.json` or database `video_matrix_state.signature_history`.
   - `max_variant_attempts` controls the retry limit. The default is 20.

7. Rendering
   - Builds an FFmpeg `filter_complex` graph.
   - Each segment is trimmed, scaled, cropped, optionally mirrored, color-balanced, and enhanced.
   - HUD, slogan, and title are drawn with FFmpeg `drawbox` and `drawtext`.
   - Optional intro and outro covers are inserted as looped still-image video segments.
   - Segments are concatenated into `[vout]`.
   - BGM is looped with `-stream_loop -1` and cut to the final video length with `-shortest`.

8. Sidecar outputs
   - Can emit MP4, PNG cover, TXT copy, and JSON manifest depending on requested output types.
   - Cover generation uses the configured cover template when available.
   - Marketing copy first tries the Spark CLI in `D:\code\Python`; if unavailable, it falls back to `config/video_matrix/copy_template.txt`.

9. Parallel execution
   - `run_pipeline()` renders variants with `ThreadPoolExecutor`.
   - Default worker count is capped by total variants, CPU count, and an upper bound of 4 workers.

## Current Important Constraint

The default composition still consumes `category_A`, `category_B`, and `category_C` to remain backward compatible. Additional categories are available once they are added to `composition_sequence` through the UI or configuration.

## V1 Extensibility Hooks

- `beat_detection` controls `mode`, BPM bounds, and fallback beat spacing without adding new audio dependencies.
- `enhancement_modules` is reserved for future AI/GPU visual enhancement modules and is disabled by default.
- `copy_mode` is reserved for future copy-generation policy. The current behavior remains Spark first, template fallback.
