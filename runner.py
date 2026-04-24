from __future__ import annotations

import logging
import shutil
import subprocess
from pathlib import Path

from config import HYDRUS_EXE, USE_PHYDRUS

LOGGER = logging.getLogger(__name__)


def _run_with_phydrus(run_dir: Path) -> tuple[bool, str]:
    try:
        import phydrus  # type: ignore  # noqa: F401
    except Exception as exc:
        return False, f"phydrus unavailable: {exc}"

    return False, "phydrus detected but direct run API is not configured; fallback to executable."


def _run_with_executable(run_dir: Path) -> tuple[bool, str]:
    exe_path = shutil.which(HYDRUS_EXE)
    if not exe_path:
        local = run_dir / HYDRUS_EXE
        if local.exists():
            exe_path = str(local)

    if not exe_path:
        return False, f"Hydrus executable not found ({HYDRUS_EXE}). Input files generated only."

    try:
        completed = subprocess.run(
            [exe_path],
            cwd=run_dir,
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode == 0:
            return True, completed.stdout.strip()[:800]
        return False, (completed.stderr or completed.stdout).strip()[:800]
    except Exception as exc:
        return False, f"Hydrus execution failed: {exc}"


def run_hydrus(run_dir: Path) -> tuple[bool, str]:
    if USE_PHYDRUS:
        ok, message = _run_with_phydrus(run_dir)
        if ok:
            return ok, message
        LOGGER.warning("%s", message)

    return _run_with_executable(run_dir)
