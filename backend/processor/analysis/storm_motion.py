from __future__ import annotations

from backend.processor.analysis.base import AnalysisResult, BaseAnalyzer, ProcessedFrame


class StormMotionAnalyzer(BaseAnalyzer):
    """Placeholder analyzer retained for future frame-to-frame motion work."""

    name = "storm_motion"

    def run(self, frame: ProcessedFrame, context: dict | None = None) -> AnalysisResult:
        return AnalysisResult(
            analyzer=self.name,
            payload={
                "status": "ok",
                "max_severity": "NONE",
                "signature_count": 0,
                "signatures": [],
            },
        )
