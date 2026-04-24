from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from config import LOG_LEVEL, OUTPUT_DIR
from data_loader import load_all_data, run_data_checks
from hydrus_builder import build_run_project
from postprocess import create_figures, export_results_to_excel, parse_run_outputs
from runner import run_hydrus


def _setup_logging() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.FileHandler(OUTPUT_DIR / "pipeline.log", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def _resolve_phy_row(phy_df: pd.DataFrame, point_id: object) -> pd.Series:
    if "point_id" in phy_df.columns:
        rows = phy_df[phy_df["point_id"] == point_id]
        if not rows.empty:
            return rows.iloc[0]
    return phy_df.iloc[0]


def _resolve_diffus(pollution_row: pd.Series) -> float:
    for col in pollution_row.index:
        if str(col).lower() == "diffus_w":
            return float(pollution_row[col])
    return 0.5


def run_pipeline() -> None:
    logger = logging.getLogger("main")
    summary_records: list[dict] = []

    try:
        data = load_all_data()
    except Exception as exc:
        logger.exception("Failed to load data: %s", exc)
        return

    run_data_checks(
        data.pet_wide,
        data.pre_wide,
        data.pet_long,
        data.pre_long,
        Path("output/data_check.txt"),
    )

    if "PFAS_Name" not in data.pollution.columns:
        data.pollution["PFAS_Name"] = [f"PFAS_{i}" for i in range(len(data.pollution))]

    point_ids = sorted(set(data.pet_wide["point_id"]).intersection(set(data.pre_wide["point_id"])))

    for point_id in point_ids:
        phy_row = _resolve_phy_row(data.phy, point_id)

        pre_series = data.pre_wide[data.pre_wide["point_id"] == point_id].iloc[0]
        pet_series = data.pet_wide[data.pet_wide["point_id"] == point_id].iloc[0]

        for _, pol_row in data.pollution.iterrows():
            pfas_name = str(pol_row.get("PFAS_Name", "Unknown"))
            diffus_w = _resolve_diffus(pol_row)

            success = False
            error_message = ""
            run_metrics = {
                "final_time_step": None,
                "nod_inf_last": None,
                "obs_node_last": None,
                "balance_last": None,
            }

            try:
                run_dir = build_run_project(
                    point_id=point_id,
                    pfas_name=pfas_name,
                    pet_wide=data.pet_wide,
                    pre_wide=data.pre_wide,
                    phy_row=phy_row,
                    diffus_w=diffus_w,
                )
                success, msg = run_hydrus(run_dir)
                if not success:
                    error_message = msg
                run_metrics = parse_run_outputs(run_dir)

                try:
                    create_figures(point_id, pfas_name, run_dir, pre_series, pet_series)
                except Exception as fig_exc:
                    logger.warning("Figure creation failed for point=%s pfas=%s: %s", point_id, pfas_name, fig_exc)
            except Exception as exc:
                error_message = str(exc)
                logger.exception("Run failed for point=%s pfas=%s", point_id, pfas_name)

            summary_records.append(
                {
                    "point_id": point_id,
                    "PFAS_Name": pfas_name,
                    "success": success,
                    "error_message": error_message,
                    "final_time_step": run_metrics.get("final_time_step"),
                    "water_summary": run_metrics.get("obs_node_last"),
                    "solute_summary": run_metrics.get("nod_inf_last"),
                    "key_metric": run_metrics.get("balance_last"),
                }
            )

    summary_df = pd.DataFrame(summary_records)
    summary_path = Path("output/run_summary.csv")
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_df.to_csv(summary_path, index=False)
    export_results_to_excel(summary_df)
    logger.info("Pipeline completed. Summary: %s", summary_path)


if __name__ == "__main__":
    _setup_logging()
    run_pipeline()
