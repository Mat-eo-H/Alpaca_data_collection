"""Utility helpers for persisting downloader state files atomically."""

from __future__ import annotations

import os
from typing import IO

import pandas as pd


def _write_dataframe(tmp_file: IO[str], frame: pd.DataFrame) -> None:
    """Write DataFrame contents to an open temporary file and fsync."""
    frame.to_csv(tmp_file)
    tmp_file.flush()
    os.fsync(tmp_file.fileno())


def persist_state_dataframe(frame: pd.DataFrame, destination: str) -> None:
    """Persist the downloader state DataFrame to disk using a temp file swap."""
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    tmp_path = destination + ".tmp"
    with open(tmp_path, "w", newline="", encoding="utf-8") as tmp_file:
        _write_dataframe(tmp_file, frame)
    os.replace(tmp_path, destination)
