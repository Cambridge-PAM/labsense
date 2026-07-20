from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, Tuple

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from Labsense_SQL.Fumehood_dashboard import calculate_sash_percentage_open

DEFAULT_UREASIL_PATH = Path(
    r"Z:\LabsenseExports\Ned Loveridge Data\Case study ureasil\UreasilCaseStudy.xlsx"
)

_METRIC_DISPLAY_NAMES = {
    "sash": "Sash Opening (%)",
    "light": "Light (lux)",
    "power": "Power (kW)",
}

_METRIC_ORDER = ("sash", "light", "power")
_GROUP_ORDER = ("Bad", "Good")

_GROUP_COLUMN_OFFSETS = {
    "Bad": 0,
    "Good": 6,
}

_LIGHT_MATCH_WINDOWS = {
    "Bad": (
        pd.Timestamp("2025-02-24 15:20:00"),
        pd.Timestamp("2025-02-24 16:00:00"),
    ),
    "Good": (
        pd.Timestamp("2024-11-27 14:30:00"),
        pd.Timestamp("2024-11-27 14:40:00"),
    ),
}


def _build_metric_frame(
    raw_df: pd.DataFrame,
    timestamp_col: int,
    value_col: int,
    value_name: str,
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Timestamp": pd.to_datetime(
                raw_df.iloc[2:, timestamp_col], errors="coerce"
            ),
            value_name: pd.to_numeric(raw_df.iloc[2:, value_col], errors="coerce"),
        }
    ).dropna(subset=["Timestamp", value_name])


def _first_five_minute_average(series_df: pd.DataFrame, value_name: str) -> float:
    first_timestamp = series_df["Timestamp"].min()
    first_five_minutes = series_df.loc[
        series_df["Timestamp"] < first_timestamp + pd.Timedelta(minutes=5),
        value_name,
    ]
    return float(first_five_minutes.mean())


def _calculate_dashboard_sash_opening(
    distance_series: pd.Series,
    lab_id: int,
    sublab_id: int,
) -> pd.Series:
    return distance_series.apply(
        lambda distance: calculate_sash_percentage_open(
            float(distance),
            lab_id=lab_id,
            sublab_id=sublab_id,
        )
    )


def _scale_case_study_light(parsed_data: Dict[str, Dict[str, pd.DataFrame]]) -> float:
    bad_start, bad_end = _LIGHT_MATCH_WINDOWS["Bad"]
    good_start, good_end = _LIGHT_MATCH_WINDOWS["Good"]

    bad_light = parsed_data["Bad"]["light"]
    good_light = parsed_data["Good"]["light"]
    light_col = _METRIC_DISPLAY_NAMES["light"]

    bad_window = bad_light.loc[
        (bad_light["Timestamp"] >= bad_start) & (bad_light["Timestamp"] <= bad_end),
        light_col,
    ]
    good_window = good_light.loc[
        (good_light["Timestamp"] >= good_start) & (good_light["Timestamp"] <= good_end),
        light_col,
    ]

    if bad_window.empty or good_window.empty:
        raise ValueError(
            "Cannot scale light data because one of the requested time windows has no samples."
        )

    bad_mean = float(bad_window.mean())
    good_mean = float(good_window.mean())
    if bad_mean == 0:
        raise ValueError(
            "Cannot scale bad light data because the reference window average is zero."
        )

    scale_factor = good_mean / bad_mean
    parsed_data["Bad"]["light"] = bad_light.assign(
        **{light_col: bad_light[light_col] * scale_factor}
    )
    return scale_factor


def load_ureasil_case_study_data(
    workbook_path: str | Path = DEFAULT_UREASIL_PATH,
    lab_id: int = 1,
    sublab_id: int = 3,
) -> Dict[str, Dict[str, pd.DataFrame]]:
    """Load the Ureasil case study workbook into bad/good sensor series.

    The workbook stores two logical header rows:
    - row 1: group labels (`Bad`, `Good`)
    - row 2: repeated `Timestamp` and metric names for each series

    Returns a nested mapping of group -> metric -> DataFrame where each DataFrame
    contains `Timestamp` plus a single value column.
    """

    workbook_path = Path(workbook_path)
    if not workbook_path.exists():
        raise FileNotFoundError(f"Workbook not found: {workbook_path}")

    raw_df = pd.read_excel(workbook_path, header=None)
    if raw_df.shape[0] < 3 or raw_df.shape[1] < 12:
        raise ValueError(
            "Unexpected workbook layout. Expected at least 3 rows and 12 columns."
        )

    parsed_data: Dict[str, Dict[str, pd.DataFrame]] = {}

    for group_name, start_col in _GROUP_COLUMN_OFFSETS.items():
        metric_frames: Dict[str, pd.DataFrame] = {}

        for offset in range(0, 6, 2):
            timestamp_col = start_col + offset
            value_col = timestamp_col + 1
            metric_name = str(raw_df.iat[1, value_col]).strip().lower()

            if metric_name not in _METRIC_DISPLAY_NAMES:
                raise ValueError(
                    f"Unexpected metric '{raw_df.iat[1, value_col]}' in {group_name} block."
                )

            value_name = _METRIC_DISPLAY_NAMES[metric_name]
            metric_df = _build_metric_frame(
                raw_df,
                timestamp_col,
                value_col,
                value_name,
            )

            metric_frames[metric_name] = metric_df

        parsed_data[group_name] = metric_frames

    good_closed_reference_mm = _first_five_minute_average(
        parsed_data["Good"]["sash"],
        "Sash Opening (%)",
    )
    bad_closed_reference_mm = _first_five_minute_average(
        parsed_data["Bad"]["sash"],
        "Sash Opening (%)",
    )
    bad_baseline_offset_mm = bad_closed_reference_mm - good_closed_reference_mm

    for group_name in _GROUP_ORDER:
        sash_df = parsed_data[group_name]["sash"].copy()

        if group_name == "Bad":
            sash_df["Sash Opening (%)"] = (
                sash_df["Sash Opening (%)"] - bad_baseline_offset_mm
            )

        sash_df["Sash Opening (%)"] = _calculate_dashboard_sash_opening(
            sash_df["Sash Opening (%)"],
            lab_id=lab_id,
            sublab_id=sublab_id,
        )
        parsed_data[group_name]["sash"] = sash_df.dropna(subset=["Sash Opening (%)"])

    _scale_case_study_light(parsed_data)

    # Convert per-minute energy style readings to average power in kW.
    for group_name in _GROUP_ORDER:
        power_col = _METRIC_DISPLAY_NAMES["power"]
        parsed_data[group_name]["power"][power_col] = (
            parsed_data[group_name]["power"][power_col] * 60.0
        )

    return parsed_data


def plot_ureasil_case_study(
    workbook_path: str | Path = DEFAULT_UREASIL_PATH,
    output_path: str | Path | None = None,
    lab_id: int = 1,
    sublab_id: int = 3,
) -> Tuple[Any, Any, Dict[str, Dict[str, pd.DataFrame]]]:
    """Plot bad/good distance, light, and power data from the case study workbook.

    Returns `(fig, axes, parsed_data)` so the caller can further customize or inspect
    the plot. If `output_path` is provided, the figure is also saved there.
    """

    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D

    # Preserve editable text in exported PDF files.
    plt.rcParams["pdf.fonttype"] = 42
    plt.rcParams["ps.fonttype"] = 42

    parsed_data = load_ureasil_case_study_data(
        workbook_path,
        lab_id=lab_id,
        sublab_id=sublab_id,
    )

    fig, base_axes = plt.subplots(
        nrows=2, ncols=1, figsize=(16, 12), constrained_layout=True
    )

    metric_colors = {
        "sash": "#1f77b4",
        "light": "#f39c12",
        "power": "#8e44ad",
    }
    title_fontsize = 22
    axis_label_fontsize = 18
    tick_fontsize = 15
    legend_fontsize = 15

    for col_index, group_name in enumerate(_GROUP_ORDER):
        group_data = parsed_data[group_name]
        sash_ax = base_axes[col_index]
        light_ax = sash_ax.twinx()
        power_ax = sash_ax.twinx()

        # Offset the third y-axis to the right so all three scales are readable.
        power_ax.spines["right"].set_position(("axes", 1.14))
        power_ax.spines["right"].set_visible(True)

        sash_ax.spines["left"].set_color(metric_colors["sash"])
        light_ax.spines["right"].set_color(metric_colors["light"])
        power_ax.spines["right"].set_color(metric_colors["power"])

        sash_df = group_data["sash"]
        light_df = group_data["light"]
        power_df = group_data["power"]

        sash_ax.plot(
            sash_df["Timestamp"],
            sash_df[_METRIC_DISPLAY_NAMES["sash"]],
            color=metric_colors["sash"],
            linewidth=1.6,
        )
        light_ax.plot(
            light_df["Timestamp"],
            light_df[_METRIC_DISPLAY_NAMES["light"]],
            color=metric_colors["light"],
            linewidth=1.4,
        )
        power_ax.plot(
            power_df["Timestamp"],
            power_df[_METRIC_DISPLAY_NAMES["power"]],
            color=metric_colors["power"],
            linewidth=1.4,
        )

        sash_ax.set_title(f"{group_name} Sensors", fontsize=title_fontsize, pad=34)
        sash_ax.set_xlabel("Time", fontsize=axis_label_fontsize)
        sash_ax.set_ylabel(
            "Sash Opening (%)",
            color=metric_colors["sash"],
            fontsize=axis_label_fontsize,
        )
        light_ax.set_ylabel(
            "Light (lux)", color=metric_colors["light"], fontsize=axis_label_fontsize
        )
        power_ax.set_ylabel(
            "Power (kW)", color=metric_colors["power"], fontsize=axis_label_fontsize
        )

        sash_ax.tick_params(
            axis="y", colors=metric_colors["sash"], labelsize=tick_fontsize
        )
        light_ax.tick_params(
            axis="y", colors=metric_colors["light"], labelsize=tick_fontsize
        )
        power_ax.tick_params(
            axis="y", colors=metric_colors["power"], labelsize=tick_fontsize
        )
        sash_ax.tick_params(axis="x", labelsize=tick_fontsize)

        sash_ax.grid(True, alpha=0.3)
        sash_ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))

        legend_handles = [
            Line2D(
                [0], [0], color=metric_colors["sash"], lw=1.8, label="Sash Opening (%)"
            ),
            Line2D([0], [0], color=metric_colors["light"], lw=1.6, label="Light (lux)"),
            Line2D([0], [0], color=metric_colors["power"], lw=1.6, label="Power (kW)"),
        ]
        sash_ax.legend(
            handles=legend_handles,
            loc="lower center",
            bbox_to_anchor=(0.5, 1.01),
            borderaxespad=0.0,
            fontsize=legend_fontsize,
            ncol=3,
            frameon=False,
        )

    if output_path is not None:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        png_path = (
            output_path.with_suffix(".png")
            if output_path.suffix
            else Path(f"{output_path}.png")
        )
        pdf_path = png_path.with_suffix(".pdf")
        fig.savefig(png_path, dpi=150, bbox_inches="tight")
        fig.savefig(pdf_path, bbox_inches="tight")

    return fig, base_axes, parsed_data


if __name__ == "__main__":
    output_file = Path("plots") / "ureasil_case_study.png"
    figure, _, _ = plot_ureasil_case_study(output_path=output_file)
    print(f"Saved plot to {output_file.resolve()}")
    figure.show()
