# Analysis Plugin Zone

This directory is the extension point for derived radar analyses that run after a frame has been ingested and rendered.

## Add a New Analysis Module

1. Implement `class MyAnalyzer(BaseAnalyzer)` in a new module.
2. Accept a `ProcessedFrame` and return an `AnalysisResult` from `run(frame: ProcessedFrame)`.
3. Keep expensive dependencies local to the analyzer module so the base processor stays lean.

## Register It

Add the analyzer to `analysis/registry.py` so the processor can instantiate it during the post-processing phase.

## Expose Output Through the API

There are two supported V1 paths:

1. Add new columns to `radar_frames` for frame-scoped scalar outputs.
2. Add a separate `analysis_results` table when an analyzer emits multiple features or geometries per frame.

The current analyzers are stubs. They exist so the repository has a clear plugin contract instead of burying future work inside the core ingestion loop.
