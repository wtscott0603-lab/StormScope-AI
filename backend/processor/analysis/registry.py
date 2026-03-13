from __future__ import annotations

from backend.processor.analysis.base import BaseAnalyzer
from backend.processor.analysis.debris import DebrisAnalyzer
from backend.processor.analysis.hail import HailAnalyzer
from backend.processor.analysis.rotation import RotationAnalyzer
from backend.processor.analysis.storm_motion import StormMotionAnalyzer
from backend.processor.analysis.wind import WindAnalyzer


def registered_analyzers() -> list[BaseAnalyzer]:
    return [
        HailAnalyzer(),
        RotationAnalyzer(),
        DebrisAnalyzer(),
        StormMotionAnalyzer(),
        WindAnalyzer(),
    ]
