"""One shared logging setup for the API and the ETL loader.

Logs to the console (so it still shows up under uvicorn or a plain
python run) and to a rotating file under logs/, so anything that
happened is still readable after the process has exited.
"""

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parent / "logs"


def setup_logging(name: str, level: int = logging.INFO) -> None:
    LOG_DIR.mkdir(exist_ok=True)

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    file_handler = RotatingFileHandler(
        LOG_DIR / f"{name}.log", maxBytes=1_000_000, backupCount=3
    )
    file_handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers = [console_handler, file_handler]
