"""Data models and result contracts for the Luna platform.

Pure standard library dataclasses with zero external dependencies.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ExtractionResult:
    """Metadata summary of an extraction process."""
    target_path: Path
    row_count: int
    batch_count: int
    file_size_bytes: int
    duration_seconds: float

    @property
    def file_size_mb(self) -> float:
        """Size of the written file in megabytes."""
        return round(self.file_size_bytes / (1024 * 1024), 2)

    @property
    def rows_per_second(self) -> float:
        """Extraction throughput in rows per second."""
        if self.duration_seconds <= 0:
            return float(self.row_count)
        return round(self.row_count / self.duration_seconds, 1)

    def __repr__(self) -> str:
        return (
            f"ExtractionResult(rows={self.row_count:,}, "
            f"row_groups={self.batch_count}, "
            f"size={self.file_size_mb} MB, "
            f"time={self.duration_seconds:.2f}s, "
            f"path='{self.target_path.name}')"
        )
