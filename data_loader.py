from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

from config import BASE_DIR, PET_PATTERN, PRE_PATTERN, PHY_FILE, POLLUTION_FILE

LOGGER = logging.getLogger(__name__)


@dataclass
class LoadedData:
    pet_wide: pd.DataFrame
    pre_wide: pd.DataFrame
    pet_long: pd.DataFrame
    pre_long: pd.DataFrame
    phy: pd.DataFrame
    pollution: pd.DataFrame


def _read_and_merge_parts(pattern: str) -> pd.DataFrame:
    files = sorted(BASE_DIR.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No files found for pattern: {pattern}")

    frames = []
    for file in files:
        LOGGER.info("Loading %s", file.name)
        frame = pd.read_csv(file)
        if "point_id" not in frame.columns:
            first_col = frame.columns[0]
            frame = frame.rename(columns={first_col: "point_id"})
        frames.append(frame)

    merged = pd.concat(frames, ignore_index=True)
    merged = merged.drop_duplicates(subset=["point_id"], keep="first")
    merged = _normalize_time_columns(merged)
    return merged


def _normalize_time_columns(df: pd.DataFrame) -> pd.DataFrame:
    renamed = {}
    for col in df.columns:
        if col == "point_id":
            continue
        try:
            renamed[col] = str(int(float(col)))
        except (ValueError, TypeError):
            renamed[col] = str(col)
    df = df.rename(columns=renamed)

    time_cols = [c for c in df.columns if c != "point_id"]
    ordered = sorted(time_cols, key=lambda c: int(c) if c.isdigit() else c)
    return df[["point_id", *ordered]]


def _wide_to_long(df: pd.DataFrame, value_name: str) -> pd.DataFrame:
    long_df = df.melt(id_vars=["point_id"], var_name="time", value_name=value_name)
    long_df["time"] = pd.to_numeric(long_df["time"], errors="coerce")
    long_df = long_df.sort_values(["point_id", "time"]).reset_index(drop=True)
    return long_df


def _read_excel(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    return pd.read_excel(path)


def run_data_checks(
    pet_wide: pd.DataFrame,
    pre_wide: pd.DataFrame,
    pet_long: pd.DataFrame,
    pre_long: pd.DataFrame,
    output_file: Path,
) -> None:
    messages: list[str] = []

    pet_points = set(pet_wide["point_id"])
    pre_points = set(pre_wide["point_id"])
    if pet_points == pre_points:
        messages.append("[OK] point_id sets are identical between PET and PRE.")
    else:
        missing_in_pet = sorted(pre_points - pet_points)
        missing_in_pre = sorted(pet_points - pre_points)
        messages.append(f"[WARN] point_id mismatch. Missing in PET: {missing_in_pet[:10]}")
        messages.append(f"[WARN] point_id mismatch. Missing in PRE: {missing_in_pre[:10]}")

    for label, df in (("PET", pet_wide), ("PRE", pre_wide)):
        time_cols = [c for c in df.columns if c != "point_id" and str(c).isdigit()]
        if not time_cols:
            messages.append(f"[WARN] {label}: no numeric time columns found.")
            continue
        times = sorted(int(c) for c in time_cols)
        expected = list(range(times[0], times[-1] + 1))
        if times == expected:
            messages.append(f"[OK] {label}: time columns are continuous from {times[0]} to {times[-1]}.")
        else:
            missing = sorted(set(expected) - set(times))
            messages.append(f"[WARN] {label}: missing time indices count={len(missing)}, sample={missing[:20]}.")

    for label, df in (("PET", pet_long), ("PRE", pre_long)):
        na_count = int(df.isna().sum().sum())
        messages.append(f"[INFO] {label}: total missing cells in long table = {na_count}.")

    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text("\n".join(messages), encoding="utf-8")
    LOGGER.info("Data checks written: %s", output_file)


def load_all_data(data_root: Path | None = None) -> LoadedData:
    _ = data_root or BASE_DIR

    pet_wide = _read_and_merge_parts(PET_PATTERN)
    pre_wide = _read_and_merge_parts(PRE_PATTERN)
    pet_long = _wide_to_long(pet_wide, "pet")
    pre_long = _wide_to_long(pre_wide, "pre")

    phy = _read_excel(BASE_DIR / PHY_FILE)
    pollution = _read_excel(BASE_DIR / POLLUTION_FILE)

    return LoadedData(
        pet_wide=pet_wide,
        pre_wide=pre_wide,
        pet_long=pet_long,
        pre_long=pre_long,
        phy=phy,
        pollution=pollution,
    )
