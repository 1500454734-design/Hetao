from __future__ import annotations

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
TEMPLATE_DIR = BASE_DIR / "template_project"
OUTPUT_DIR = BASE_DIR / "output"
RUNS_DIR = OUTPUT_DIR / "runs"
FIGURES_DIR = OUTPUT_DIR / "figures"
RESULTS_DIR = OUTPUT_DIR / "results"

PET_PATTERN = "Pet_part*.csv"
PRE_PATTERN = "Pre_part*.csv"
PHY_FILE = "phy.xls"
POLLUTION_FILE = "pollution.xls"

HYDRUS_EXE = os.getenv("HYDRUS_EXE", "Hydrus1D.exe")
USE_PHYDRUS = os.getenv("USE_PHYDRUS", "1") == "1"

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
RUNS_DIR.mkdir(parents=True, exist_ok=True)
FIGURES_DIR.mkdir(parents=True, exist_ok=True)
RESULTS_DIR.mkdir(parents=True, exist_ok=True)
