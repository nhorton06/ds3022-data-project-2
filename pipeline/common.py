"""Shared settings, paths, and logging used by every task."""

import logging
from pathlib import Path

from prefect import get_run_logger

# Package set A
REPOS = ["numpy/numpy", "networkx/networkx", "statsmodels/statsmodels"]
WINDOW_START = "2024-01-01T00:00:00Z"

PROJECT_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_DIR / "data" / "raw"
LOADED_MARKER = RAW_DIR / "_LOADED"
DB_PATH = PROJECT_DIR / "packages.duckdb"
LOG_DIR = PROJECT_DIR / "logs"


class TaskLogger:
    """Send each message to the Prefect run logger and to logs/<name>.log."""

    def __init__(self, name):
        self.prefect = get_run_logger()
        self.file = logging.getLogger(f"pipeline.{name}")
        if not self.file.handlers:
            LOG_DIR.mkdir(exist_ok=True)
            handler = logging.FileHandler(LOG_DIR / f"{name}.log")
            handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
            self.file.addHandler(handler)
            self.file.setLevel(logging.INFO)
            self.file.propagate = False

    def info(self, msg):
        self.prefect.info(msg)
        self.file.info(msg)

    def warning(self, msg):
        self.prefect.warning(msg)
        self.file.warning(msg)
