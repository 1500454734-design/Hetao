from __future__ import annotations

import logging
import re
from pathlib import Path

import matplotlib
import pandas as pd

from config import FIGURES_DIR, RESULTS_DIR

matplotlib.use("Agg")
import matplotlib.pyplot as plt
LOGGER = logging.getLogger(__name__)


_FLOAT_RE = re.compile(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?")


def _extract_last_float(path: Path) -> float | None:
    if not path.exists():
        return None
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    for line in reversed(lines):
        vals = _FLOAT_RE.findall(line)
        if vals:
            return float(vals[-1])
    return None


def parse_run_outputs(run_dir: Path) -> dict:
    nod = run_dir / "Nod_Inf.out"
    obs = run_dir / "Obs_Node.out"
    bal = run_dir / "Balance.out"

    metrics = {
        "nod_inf_last": _extract_last_float(nod),
        "obs_node_last": _extract_last_float(obs),
        "balance_last": _extract_last_float(bal),
    }

    final_time_step = None
    if obs.exists():
        for line in reversed(obs.read_text(encoding="utf-8", errors="ignore").splitlines()):
            vals = _FLOAT_RE.findall(line)
            if vals:
                final_time_step = float(vals[0])
                break
    metrics["final_time_step"] = final_time_step
    return metrics


def create_figures(
    point_id: object,
    pfas_name: str,
    run_dir: Path,
    pre_series: pd.Series,
    pet_series: pd.Series,
) -> None:
    fig_dir = FIGURES_DIR / f"point_{point_id}" / f"pfas_{pfas_name}"
    fig_dir.mkdir(parents=True, exist_ok=True)

    time_cols = [c for c in pre_series.index if c != "point_id" and str(c).isdigit()]
    times = sorted([int(c) for c in time_cols])
    pre_vals = [float(pre_series[str(t)]) for t in times]
    pet_vals = [float(pet_series[str(t)]) for t in times]

    plt.figure(figsize=(10, 4))
    plt.plot(times, pre_vals, label="Precipitation")
    plt.plot(times, pet_vals, label="PET")
    plt.xlabel("Time")
    plt.ylabel("Value")
    plt.title(f"Point {point_id} - Precipitation vs PET")
    plt.legend()
    plt.tight_layout()
    plt.savefig(fig_dir / "pre_vs_pet.png", dpi=150)
    plt.close()

    obs_file = run_dir / "Obs_Node.out"
    if obs_file.exists():
        data = []
        for line in obs_file.read_text(encoding="utf-8", errors="ignore").splitlines():
            vals = _FLOAT_RE.findall(line)
            if len(vals) >= 2:
                data.append((float(vals[0]), float(vals[1])))
        if data:
            df = pd.DataFrame(data, columns=["time", "value"])
            plt.figure(figsize=(10, 4))
            plt.plot(df["time"], df["value"])
            plt.xlabel("Time")
            plt.ylabel("Water content / observed variable")
            plt.title(f"Point {point_id} - Water content change")
            plt.tight_layout()
            plt.savefig(fig_dir / "water_content.png", dpi=150)
            plt.close()

    nod_file = run_dir / "Nod_Inf.out"
    if nod_file.exists():
        data = []
        for line in nod_file.read_text(encoding="utf-8", errors="ignore").splitlines():
            vals = _FLOAT_RE.findall(line)
            if len(vals) >= 2:
                data.append((float(vals[0]), float(vals[-1])))
        if data:
            df = pd.DataFrame(data, columns=["x", "conc"])
            plt.figure(figsize=(10, 4))
            plt.plot(df["x"], df["conc"])
            plt.xlabel("Profile / Time index")
            plt.ylabel("Concentration")
            plt.title(f"Point {point_id} - Solute concentration")
            plt.tight_layout()
            plt.savefig(fig_dir / "solute_concentration.png", dpi=150)
            plt.close()


def _sanitize_sheet_name(name: str) -> str:
    cleaned = re.sub(r"[\\/*?:\[\]]", "_", str(name)).strip()
    cleaned = cleaned[:31] if cleaned else "Sheet"
    return cleaned


def export_results_to_excel(summary_df: pd.DataFrame) -> tuple[Path, Path]:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    xlsx_path = RESULTS_DIR / "final_results.xlsx"
    mapping_path = RESULTS_DIR / "sheet_name_mapping.csv"

    mappings = []
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        if summary_df.empty:
            pd.DataFrame(
                [{"message": "No runs were executed. Please check input files."}]
            ).to_excel(writer, sheet_name="Overview", index=False)
            mappings.append({"PFAS_Name": "N/A", "sheet_name": "Overview"})

        for pfas_name, group in summary_df.groupby("PFAS_Name", dropna=False):
            original = str(pfas_name)
            sheet_name = _sanitize_sheet_name(original)
            if sheet_name in {m["sheet_name"] for m in mappings}:
                sheet_name = f"{sheet_name[:28]}_{len(mappings)}"

            export_cols = [
                "point_id",
                "PFAS_Name",
                "success",
                "error_message",
                "final_time_step",
                "water_summary",
                "solute_summary",
                "key_metric",
            ]
            for col in export_cols:
                if col not in group.columns:
                    group[col] = None
            group[export_cols].to_excel(writer, sheet_name=sheet_name, index=False)
            mappings.append({"PFAS_Name": original, "sheet_name": sheet_name})

    pd.DataFrame(mappings).to_csv(mapping_path, index=False)
    LOGGER.info("Excel written: %s", xlsx_path)
    return xlsx_path, mapping_path
