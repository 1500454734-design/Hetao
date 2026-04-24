from __future__ import annotations

import logging
import re
import shutil
from pathlib import Path

import pandas as pd

from config import RUNS_DIR, TEMPLATE_DIR

LOGGER = logging.getLogger(__name__)


def _get_point_row(df: pd.DataFrame, point_id: object) -> pd.Series:
    rows = df[df["point_id"] == point_id]
    if rows.empty:
        raise KeyError(f"point_id {point_id} not found")
    return rows.iloc[0]


def _layer_count_from_phy(phy_row: pd.Series) -> int:
    count = 0
    for col in phy_row.index:
        m = re.match(r"qs_(\d+)", str(col))
        if m:
            count = max(count, int(m.group(1)))
    return max(count, 1)


def _build_atmos_lines(pre_row: pd.Series, pet_row: pd.Series) -> list[str]:
    time_cols = [c for c in pre_row.index if c != "point_id" and str(c).isdigit()]
    times = sorted(time_cols, key=lambda x: int(str(x)))

    lines = [
        "Pcp_File_Version=4\n",
        "*** BLOCK I: ATMOSPHERIC INFORMATION  **********************************\n",
        "   MaxAL                    (MaxAL = number of atmospheric data-records)\n",
        f"   {len(times)}\n",
        " DailyVar  SinusVar  lLay  lBCCycles lInterc lDummy  lDummy  lDummy  lDummy  lDummy\n",
        "       f       f       f       t       f       f       f       f       f       f\n",
        " Number of Cycles\n",
        "       1\n",
        " hCritS                 (max. allowed pressure head at the soil surface)\n",
        "      0\n",
        "       tAtm        Prec       rSoil       rRoot      hCritA          rB          hB          ht        tTop        tBot        Ampl        cTop        cBot   RootDepth\n",
    ]

    for t in times:
        pre = float(pre_row[t]) if pd.notna(pre_row[t]) else 0.0
        pet = float(pet_row[t]) if pd.notna(pet_row[t]) else 0.0
        lines.append(
            f"{int(t):11d}{pre:12.6f}{pet:12.6f}{0:12.0f}{-100:12.0f}{0:12.0f}{0:12.0f}{0:12.0f}{0:12.0f}{0:12.0f}{0:12.0f}{0:12.0f}{0:12.0f}\n"
        )
    return lines


def _update_selector(selector_path: Path, phy_row: pd.Series, diffus_w: float, tmax: int) -> None:
    lines = selector_path.read_text(encoding="utf-8", errors="ignore").splitlines(keepends=True)

    nmat = _layer_count_from_phy(phy_row)

    for i, line in enumerate(lines):
        if "NMat" in line and "NLay" in line and i + 1 < len(lines):
            lines[i + 1] = f"  {nmat:<6d}  1       1\n"
            break

    for i, line in enumerate(lines):
        if "tInit" in line and "tMax" in line and i + 1 < len(lines):
            parts = lines[i + 1].split()
            tinit = parts[0] if parts else "0.001"
            lines[i + 1] = f"      {tinit}        {tmax}\n"
            break

    for i, line in enumerate(lines):
        if "thr" in line and "ths" in line and "Alfa" in line:
            start = i + 1
            end = start + nmat
            new_rows = []
            for layer in range(1, nmat + 1):
                thr = float(phy_row.get(f"mat_bottocr_{layer}", 0.05))
                ths = float(phy_row.get(f"qs_{layer}", 0.40))
                alpha = float(phy_row.get(f"alpha_{layer}", 0.01))
                n_val = float(phy_row.get(f"n_{layer}", 1.4))
                ks = float(phy_row.get(f"ks_{layer}", 0.1))
                l_val = float(phy_row.get(f"l_{layer}", 0.5))
                new_rows.append(f"{thr:<8.6f} {ths:<8.6f} {alpha:<10.6f} {n_val:<8.6f} {ks:<10.6f} {l_val:<8.3f}\n")
            lines[start:end] = new_rows
            break

    for i, line in enumerate(lines):
        if "DifW" in line and "DifG" in line and i + 1 < len(lines):
            lines[i + 1] = f"   {diffus_w:<12.6f}           0 \n"
            break

    selector_path.write_text("".join(lines), encoding="utf-8")


def build_run_project(
    point_id: object,
    pfas_name: str,
    pet_wide: pd.DataFrame,
    pre_wide: pd.DataFrame,
    phy_row: pd.Series,
    diffus_w: float,
) -> Path:
    run_dir = RUNS_DIR / f"point_{point_id}" / f"pfas_{pfas_name}"
    if run_dir.exists():
        shutil.rmtree(run_dir)
    shutil.copytree(TEMPLATE_DIR, run_dir)

    pet_row = _get_point_row(pet_wide, point_id)
    pre_row = _get_point_row(pre_wide, point_id)

    atmos_lines = _build_atmos_lines(pre_row, pet_row)
    (run_dir / "ATMOSPH.IN").write_text("".join(atmos_lines), encoding="utf-8")

    numeric_times = [int(c) for c in pre_row.index if c != "point_id" and str(c).isdigit()]
    tmax = max(numeric_times) if numeric_times else 1
    _update_selector(run_dir / "SELECTOR.IN", phy_row, diffus_w=float(diffus_w), tmax=tmax)

    LOGGER.info("Built Hydrus input for point=%s pfas=%s", point_id, pfas_name)
    return run_dir
