"""Generate Fumehood dashboard from SQL Server data.

Queries the labsense SQL Server database for fumehood sensor data (distance and light)
grouped by laboratory and sublaboratory, and creates visualizations and an HTML dashboard.
"""

import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# Add repository root to sys.path to allow absolute imports when running this file directly
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Load environment variables from .env file at repo root
env_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(env_path)

import pyodbc
import pandas as pd
import argparse
from datetime import datetime, timedelta
from typing import Optional, Dict, Tuple
import re

# SQL Server connection details
SQL_SERVER_NAME = "MSM-FPM-70203\\LABSENSE"
DATABASE_NAME = "labsense"
TRUSTED_CONNECTION = "yes"
ENCRYPTION_PREF = "Optional"

CONNECTION_STRING = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={SQL_SERVER_NAME};"
    f"DATABASE={DATABASE_NAME};"
    f"Trusted_Connection={TRUSTED_CONNECTION};"
    f"Encrypt={ENCRYPTION_PREF}"
)

# Lab ID to name mapping
LAB_NAMES = {1: "Lab -1.025", 2: "Lab -1.041"}

# Sash opening threshold percentage (for determining "open" state)
SASH_OPEN_THRESHOLD_PERCENT = 2.0

# Fumehood calibration data: {(lab_id, sublab_id): {"fully_closed_mm": mm, "fully_open_mm": mm}}
# Distance values for sash fully closed (0% open) and fully open (100% open)
FUMEHOOD_CALIBRATION = {
    (1, 3): {"fully_closed_mm": 765, "fully_open_mm": 100},
}

# Light threshold configuration: {(lab_id, sublab_id): {"light_on_threshold_lux": lux, "room_light_on_threshold_lux": lux}}
# Light level above light_on_threshold indicates the fumehood light is on
# Light level above room_light_on_threshold indicates the room lights are on
LIGHT_THRESHOLDS = {
    (1, 3): {"light_on_threshold_lux": 80, "room_light_on_threshold_lux": 15},
}


def get_lab_display_name(lab_id: int) -> str:
    """Get display name for a lab ID."""
    return LAB_NAMES.get(lab_id, f"Lab {lab_id}")


def get_display_label(lab_id: int, sublab_id: int) -> str:
    """Get formatted display label for a lab/sublab combination."""
    lab_name = get_lab_display_name(lab_id)
    return f"Fumehood {sublab_id} ({lab_name})"


def calculate_sash_percentage_open(
    distance: float, lab_id: int, sublab_id: int
) -> Optional[float]:
    """Calculate sash opening percentage based on distance.

    Args:
        distance: Distance reading in mm
        lab_id: Laboratory ID
        sublab_id: Sublaboratory/Fumehood ID

    Returns:
        Percentage open (0-100) or None if no calibration data or invalid result
    """
    if (lab_id, sublab_id) not in FUMEHOOD_CALIBRATION:
        return None

    cal = FUMEHOOD_CALIBRATION[(lab_id, sublab_id)]
    fully_closed = cal["fully_closed_mm"]
    fully_open = cal["fully_open_mm"]

    # Clamp distance to fully_closed if it's greater (sensor noise, mechanical tolerance)
    # Any distance >= fully_closed is treated as fully closed (0% open)
    distance = min(distance, fully_closed)

    # Calculate percentage open
    # When distance = fully_closed, % = 0
    # When distance = fully_open, % = 100
    percentage = ((fully_closed - distance) / (fully_closed - fully_open)) * 100

    # Only return valid percentages (0-100)
    if 0 <= percentage <= 100:
        return percentage
    return None


def is_light_on(light_level: float, lab_id: int, sublab_id: int) -> Optional[bool]:
    """Check if the fumehood light is on based on light level.

    Args:
        light_level: Light level reading in lux
        lab_id: Laboratory ID
        sublab_id: Sublaboratory/Fumehood ID

    Returns:
        True if light is on, False if off, or None if no threshold data
    """
    if (lab_id, sublab_id) not in LIGHT_THRESHOLDS:
        return None

    threshold = LIGHT_THRESHOLDS[(lab_id, sublab_id)]["light_on_threshold_lux"]
    return light_level > threshold


def get_room_light_presence_data(
    light_df: pd.DataFrame, lab_id: int, sublab_id: int
) -> Optional[pd.DataFrame]:
    """Prepare room light presence data using the configured room-light threshold.

    Presence is considered on when light exceeds room_light_on_threshold_lux.

    Args:
        light_df: DataFrame with Timestamp and Light columns
        lab_id: Laboratory ID
        sublab_id: Sublaboratory/Fumehood ID

    Returns:
        Sorted DataFrame with a Presence column containing 1 or 0, or None if
        no room-light threshold is configured.
    """
    thresholds = LIGHT_THRESHOLDS.get((lab_id, sublab_id))
    if thresholds is None:
        return None

    threshold = thresholds.get("room_light_on_threshold_lux")
    if threshold is None or light_df.empty:
        return None

    light_sorted = light_df.sort_values("Timestamp").reset_index(drop=True).copy()
    light_sorted["Presence"] = (light_sorted["Light"] > threshold).astype(int)
    return light_sorted


def identify_light_errors(df: pd.DataFrame) -> pd.Series:
    """Identify light reading errors based on consecutive zeros and values above 500 lux.

    A light reading of 0 is an error if it occurs for less than 10 consecutive data points.
    If 0 occurs for 10 or more consecutive points, those are valid (actual darkness).
    Light readings above 500 lux are always considered errors.

    Args:
        df: DataFrame with Light column, sorted by Timestamp

    Returns:
        Boolean Series indicating which rows are errors (True = error)
    """
    if df.empty or "Light" not in df.columns:
        return pd.Series([False] * len(df), index=df.index)

    is_error = pd.Series([False] * len(df), index=df.index)

    # Mark values above 500 lux as errors
    is_error = is_error | (df["Light"] > 500)

    # Find consecutive runs of zeros
    is_zero = (df["Light"] == 0).values

    i = 0
    while i < len(is_zero):
        if is_zero[i]:
            # Start of a zero run
            run_start = i
            run_end = i

            # Find the end of this run
            while run_end < len(is_zero) and is_zero[run_end]:
                run_end += 1

            run_length = run_end - run_start

            # If run is less than 10, mark as errors
            if run_length < 10:
                is_error.iloc[run_start:run_end] = True

            i = run_end
        else:
            i += 1

    return is_error


def identify_distance_errors(df: pd.DataFrame) -> pd.Series:
    """Identify distance reading errors based on negative readings and subsequent values.

    Distance readings are errors if:
    - The value is negative, OR
    - The value is within the next 4 readings after a negative value

    Args:
        df: DataFrame with Distance column, sorted by Timestamp

    Returns:
        Boolean Series indicating which rows are errors (True = error)
    """
    if df.empty or "Distance" not in df.columns:
        return pd.Series([False] * len(df), index=df.index)

    is_error = pd.Series([False] * len(df), index=df.index)
    distances = df["Distance"].values

    # Iterate through all readings
    for i in range(len(distances)):
        # If we find a negative value
        if distances[i] < 0:
            # Mark the negative value as error
            is_error.iloc[i] = True
            # Mark the next 4 consecutive values as errors
            for j in range(i + 1, min(i + 5, len(distances))):
                is_error.iloc[j] = True

    return is_error


def fetch_fumehood_data(connection_string: str) -> pd.DataFrame:
    """Fetch all fumehood data from SQL Server."""
    try:
        connection = pyodbc.connect(connection_string)
        query = """
            SELECT id, LabId, SubLabId, Distance, Light, Airflow, Timestamp
            FROM dbo.fumehood
            ORDER BY Timestamp DESC
        """
        df = pd.read_sql(query, connection)
        connection.close()

        # Convert Timestamp to datetime
        df["Timestamp"] = pd.to_datetime(df["Timestamp"])

        return df
    except pyodbc.Error as ex:
        print(f"Error fetching fumehood data: {ex}")
        return pd.DataFrame()


def add_night_shading(ax, light_df: pd.DataFrame, lab_id: int, sublab_id: int) -> None:
    """Add grey shading to indicate periods when room lights are off (below threshold).

    Args:
        ax: Matplotlib axis to shade
        light_df: DataFrame with Timestamp and Light columns
        lab_id: Laboratory ID
        sublab_id: Sublaboratory ID
    """
    presence_df = get_room_light_presence_data(light_df, lab_id, sublab_id)
    if presence_df is None:
        return

    # Find the start and end of each period below threshold
    i = 0
    while i < len(presence_df):
        if presence_df.loc[i, "Presence"] == 0:
            # Start of a dark period
            period_start = presence_df.loc[i, "Timestamp"]

            # Find the end of this dark period
            j = i
            while j < len(presence_df) and presence_df.loc[j, "Presence"] == 0:
                j += 1

            # Shade from period_start to the last timestamp in this period
            if j > i:
                period_end = presence_df.loc[j - 1, "Timestamp"]
                ax.axvspan(period_start, period_end, alpha=0.1, color="grey", zorder=0)

            i = j
        else:
            i += 1


def add_presence_subplot(
    ax, light_df: pd.DataFrame, lab_id: int, sublab_id: int
) -> None:
    """Plot room-light-based lab presence as a thin 0/1 strip over time."""
    presence_df = get_room_light_presence_data(light_df, lab_id, sublab_id)
    if presence_df is None:
        ax.set_visible(False)
        return

    ax.step(
        presence_df["Timestamp"],
        presence_df["Presence"],
        where="post",
        color="#2c3e50",
        linewidth=1.2,
    )
    ax.fill_between(
        presence_df["Timestamp"],
        presence_df["Presence"],
        step="post",
        alpha=0.15,
        color="#95a5a6",
    )
    ax.set_ylabel("Presence")
    ax.set_ylim(-0.1, 1.1)
    ax.set_yticks([0, 1])
    ax.grid(True, axis="y", alpha=0.3)
    ax.set_title("Lab Presence")


def add_fumehood_light_shading(
    ax, light_df: pd.DataFrame, lab_id: int, sublab_id: int
) -> None:
    """Add orange shading to indicate periods when fumehood light is on (above threshold).

    Args:
        ax: Matplotlib axis to shade
        light_df: DataFrame with Timestamp and Light columns
        lab_id: Laboratory ID
        sublab_id: Sublaboratory ID
    """
    # Check if threshold is available
    if (lab_id, sublab_id) not in LIGHT_THRESHOLDS:
        return

    if "light_on_threshold_lux" not in LIGHT_THRESHOLDS[(lab_id, sublab_id)]:
        return

    if light_df.empty:
        return

    threshold = LIGHT_THRESHOLDS[(lab_id, sublab_id)]["light_on_threshold_lux"]

    # Sort by timestamp
    light_sorted = light_df.sort_values("Timestamp").reset_index(drop=True)

    # Find periods where light is above threshold
    light_sorted["above_threshold"] = light_sorted["Light"] > threshold

    # Find the start and end of each period above threshold
    i = 0
    while i < len(light_sorted):
        if light_sorted.loc[i, "above_threshold"]:
            # Start of a light-on period
            period_start = light_sorted.loc[i, "Timestamp"]

            # Find the end of this light-on period
            j = i
            while j < len(light_sorted) and light_sorted.loc[j, "above_threshold"]:
                j += 1

            # Shade from period_start to the last timestamp in this period
            if j > i:
                period_end = light_sorted.loc[j - 1, "Timestamp"]
                ax.axvspan(
                    period_start, period_end, alpha=0.15, color="orange", zorder=0
                )

            i = j
        else:
            i += 1


def configure_time_axis(ax, mdates_module) -> None:
    """Configure the shared time axis to show midnight and noon ticks."""
    ax.xaxis.set_major_locator(mdates_module.HourLocator(byhour=[0, 12]))
    ax.xaxis.set_major_formatter(mdates_module.DateFormatter("%Y-%m-%d %H:%M"))
    ax.tick_params(axis="x", labelrotation=30)


def get_daily_presence_hours(presence_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate room-light presence to daily hours present."""
    daily_presence = presence_df.copy()
    daily_presence["Day"] = daily_presence["Timestamp"].dt.floor("D")
    daily_hours = (
        daily_presence.groupby("Day", as_index=False)["Presence"]
        .mean()
        .rename(columns={"Presence": "HoursPresent"})
    )
    daily_hours["HoursPresent"] = daily_hours["HoursPresent"] * 24
    return daily_hours


def add_daily_presence_hours_subplot(ax, presence_df: pd.DataFrame) -> None:
    """Plot daily lab presence hours as a bar chart."""
    daily_hours_df = get_daily_presence_hours(presence_df)
    if daily_hours_df.empty:
        ax.set_visible(False)
        return

    day_labels = daily_hours_df["Day"].dt.strftime("%Y-%m-%d")
    day_positions = range(len(daily_hours_df))

    ax.bar(
        day_positions,
        daily_hours_df["HoursPresent"],
        width=0.8,
        color="#16a085",
        alpha=0.8,
        align="center",
    )
    ax.set_ylabel("Presence\n(hrs/day)")
    ax.set_ylim(0, 12)
    ax.set_xticks(list(day_positions))
    ax.set_xticklabels(day_labels, rotation=0, ha="center")
    ax.grid(True, axis="y", alpha=0.3)
    ax.set_title("Lab Presence by Day")


def create_plots(df: pd.DataFrame, plot_dir: Path) -> Dict[Tuple[int, int], str]:
    """Create visualization plots for distance and light by lab/sublab."""
    try:
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
    except ImportError:
        print("matplotlib not available - skipping plots")
        return {}

    plot_dir = Path(plot_dir)
    plot_dir.mkdir(parents=True, exist_ok=True)
    plot_files = {}

    if df.empty:
        return plot_files

    # Filter to last 7 days
    last_week = datetime.now() - timedelta(days=7)
    df = df[df["Timestamp"] >= last_week]  # type: ignore[assignment]

    if df.empty:
        print("No data found in the last 7 days")
        return plot_files

    # Get unique lab/sublab combinations
    lab_sublab_combinations = df[["LabId", "SubLabId"]].drop_duplicates()

    for _, row in lab_sublab_combinations.iterrows():
        lab_id = int(row["LabId"])
        sublab_id = int(row["SubLabId"])
        key = (lab_id, sublab_id)

        # Filter data for this lab/sublab
        lab_df = df[
            (df["LabId"] == lab_id) & (df["SubLabId"] == sublab_id)
        ].sort_values(
            by=["Timestamp"]
        )  # type: ignore[call-overload]

        if lab_df.empty:
            continue

        # Count errors separately
        distance_error_mask = identify_distance_errors(lab_df)
        distance_errors = distance_error_mask.sum()

        # Identify light errors using consecutive zeros logic
        light_error_mask = identify_light_errors(lab_df)
        light_errors = light_error_mask.sum()

        # Filter data separately by sensor type
        distance_df = lab_df[~distance_error_mask]
        light_df = lab_df[~light_error_mask]

        # Check if we have any valid data for either sensor
        if distance_df.empty and light_df.empty:
            continue

        # Check if we have calibration data for this fumehood
        has_calibration = (lab_id, sublab_id) in FUMEHOOD_CALIBRATION

        # Prepare distance data for plotting
        plot_distance_df = distance_df.copy() if not distance_df.empty else None
        if has_calibration and plot_distance_df is not None:
            plot_distance_df["SashPercentOpen"] = plot_distance_df["Distance"].apply(
                lambda d: calculate_sash_percentage_open(d, lab_id, sublab_id)
            )
            plot_distance_df = plot_distance_df[
                plot_distance_df["SashPercentOpen"].notna()
            ]
            if plot_distance_df.empty:
                plot_distance_df = None

        presence_df = get_room_light_presence_data(light_df, lab_id, sublab_id)

        if presence_df is not None:
            fig, (ax1, ax2, ax3, ax_gap, ax4) = plt.subplots(
                5,
                1,
                figsize=(12, 14),
                gridspec_kw={"hspace": 0.3, "height_ratios": [3, 3, 1.1, 0.55, 1.8]},
            )
            ax2.sharex(ax1)
            ax3.sharex(ax1)
            ax_gap.axis("off")
        else:
            fig, (ax1, ax2) = plt.subplots(
                2, 1, figsize=(12, 10), sharex=True, gridspec_kw={"hspace": 0.3}
            )
            ax3 = None
            ax4 = None

        # Plot Sash Opening Percentage or Distance
        if has_calibration and plot_distance_df is not None:
            ax1.plot(
                plot_distance_df["Timestamp"],
                plot_distance_df["SashPercentOpen"],
                marker="x",
                linestyle="none",
                color="#3498db",
                linewidth=2,
                markersize=4,
            )
            ax1.set_ylabel("Sash Opening (%)")
            ax1.set_ylim(0, 100)
            ax1.set_title(
                f"{get_display_label(lab_id, sublab_id)}: Sash Opening ({distance_errors} distance errors excluded)"
            )
        elif not distance_df.empty:
            ax1.plot(
                distance_df["Timestamp"],
                distance_df["Distance"],
                marker="x",
                linestyle="none",
                color="#3498db",
                linewidth=2,
                markersize=4,
            )
            ax1.set_ylabel("Distance (mm)")
            ax1.set_title(
                f"{get_display_label(lab_id, sublab_id)}: Sash Raw Distance ({distance_errors} distance errors excluded)"
            )
        ax1.grid(True, alpha=0.3)
        configure_time_axis(ax1, mdates)

        # Plot Light
        if not light_df.empty:
            ax2.plot(
                light_df["Timestamp"],
                light_df["Light"],
                marker="x",
                linestyle="none",
                color="#e74c3c",
                linewidth=2,
                markersize=4,
            )
        ax2.set_ylabel("Light (lux)")
        ax2.set_title(
            f"{get_display_label(lab_id, sublab_id)}: Light Level ({light_errors} light errors excluded)"
        )
        ax2.grid(True, alpha=0.3)
        configure_time_axis(ax2, mdates)

        if ax3 is not None and ax4 is not None and presence_df is not None:
            add_presence_subplot(ax3, light_df, lab_id, sublab_id)
            configure_time_axis(ax3, mdates)
            add_daily_presence_hours_subplot(ax4, presence_df)
            ax4.set_xlabel("Date")
        else:
            ax2.set_xlabel("Date Time")

        # Add shading for periods when room lights are off (based on light sensor)
        if not light_df.empty:
            add_night_shading(ax1, light_df, lab_id, sublab_id)
            add_night_shading(ax2, light_df, lab_id, sublab_id)
            add_fumehood_light_shading(ax1, light_df, lab_id, sublab_id)
            add_fumehood_light_shading(ax2, light_df, lab_id, sublab_id)

        if ax3 is not None:
            ax1.tick_params(axis="x", which="both", labelbottom=False)
            ax2.tick_params(axis="x", which="both", labelbottom=False)
            ax3.set_xlabel("Date Time")
            ax3.xaxis.labelpad = 10
            ax3.tick_params(axis="x", which="both", labelbottom=True, pad=8)
            plt.setp(ax3.get_xticklabels(), visible=True, rotation=30, ha="right")

        fig.tight_layout()

        # Save plot
        plot_file = plot_dir / f"fumehood_lab{lab_id}_sublab{sublab_id}.png"
        fig.savefig(plot_file, dpi=150, bbox_inches="tight")
        plt.close(fig)

        plot_files[key] = plot_file.name

    return plot_files


def create_html_dashboard(
    df: pd.DataFrame,
    plot_files: Dict[Tuple[int, int], str],
    plot_dir: Path,
    out_file: Optional[Path] = None,
):
    """Create an HTML dashboard for Fumehood data."""

    plot_dir = Path(plot_dir)

    # Generate timestamp
    generated_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    html_lines = [
        "<!doctype html>",
        '<html lang="en">',
        "<head>",
        '  <meta charset="utf-8" />',
        "  <title>Fumehood Dashboard</title>",
        "  <style>",
        "    body { font-family: Arial, Helvetica, sans-serif; margin: 20px; background: #f5f5f5; }",
        "    .container { max-width: 1400px; margin: 0 auto; }",
        "    .header { background: white; border-radius: 10px; padding: 30px; margin-bottom: 30px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }",
        "    .header h1 { margin: 0 0 10px 0; color: #2c3e50; }",
        "    .header p { margin: 0; color: #7f8c8d; }",
        "    .lab-section { background: white; border-radius: 10px; padding: 25px; margin-bottom: 25px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }",
        "    .lab-section h2 { margin: 0 0 20px 0; color: #34495e; border-bottom: 2px solid #3498db; padding-bottom: 10px; }",
        "    .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-bottom: 20px; }",
        "    .stat-card { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 8px; }",
        "    .stat-card.distance { background: linear-gradient(135deg, #3498db 0%, #2980b9 100%); }",
        "    .stat-card.light { background: linear-gradient(135deg, #f39c12 0%, #e67e22 100%); }",
        "    .stat-card h3 { margin: 0 0 5px 0; font-size: 0.9em; opacity: 0.9; }",
        "    .stat-card .value { font-size: 2em; font-weight: bold; }",
        "    .stat-card .unit { font-size: 0.8em; opacity: 0.9; }",
        "    img { max-width: 100%; height: auto; border-radius: 8px; margin: 15px 0; }",
        "    table { border-collapse: collapse; width: 100%; margin-top: 15px; }",
        "    th, td { padding: 10px; border: 1px solid #ddd; text-align: left; }",
        "    th { background: #3498db; color: white; }",
        "    tr:nth-child(even) { background: #f9f9f9; }",
        "    .summary { background: #ecf0f1; padding: 15px; border-radius: 8px; margin-bottom: 20px; }",
        "    .summary h3 { margin: 0 0 10px 0; color: #2c3e50; }",
        "  </style>",
        "</head>",
        "<body>",
        '  <div class="container">',
        '    <div class="header">',
        "      <h1>💨 Fumehood Monitoring Dashboard</h1>",
        f"      <p>Real-time sash height and light level monitoring • Generated {generated_time}</p>",
        "    </div>",
    ]

    if df.empty:
        html_lines += [
            "    <p>No data available.</p>",
            "  </div>",
            "</body>",
            "</html>",
        ]
    else:
        # Filter to last 7 days
        last_week = datetime.now() - timedelta(days=7)
        df = df[df["Timestamp"] >= last_week]  # type: ignore[assignment]

        if df.empty:
            html_lines += [
                "    <p>No data found in the last 7 days.</p>",
                "  </div>",
                "</body>",
                "</html>",
            ]
        else:
            # Get unique lab/sublab combinations
            lab_sublab_combinations = (
                df[["LabId", "SubLabId"]].drop_duplicates().sort_values(by=["LabId", "SubLabId"])  # type: ignore[call-overload]
            )

        # Process each lab/sublab combination
        for _, row in lab_sublab_combinations.iterrows():
            lab_id = int(row["LabId"])
            sublab_id = int(row["SubLabId"])
            key = (lab_id, sublab_id)

            # Filter data for this lab/sublab
            lab_df = df[
                (df["LabId"] == lab_id) & (df["SubLabId"] == sublab_id)
            ].sort_values(
                by=["Timestamp"], ascending=False
            )  # type: ignore[call-overload]

            if lab_df.empty:
                continue

            # Count errors separately
            distance_error_mask = identify_distance_errors(lab_df)
            distance_errors = distance_error_mask.sum()

            # Identify light errors using consecutive zeros logic
            light_error_mask = identify_light_errors(lab_df)
            light_errors = light_error_mask.sum()

            # Filter to valid data separately for each sensor
            valid_distance_df = lab_df[~distance_error_mask]
            valid_light_df = lab_df[~light_error_mask]

            # Get latest valid readings
            latest_distance = None
            latest_light = None
            latest_airflow = None
            latest_time = lab_df.iloc[0]["Timestamp"]  # Use latest time from all data

            if not valid_distance_df.empty:
                latest_distance = valid_distance_df.iloc[0]["Distance"]
            else:
                latest_distance = lab_df.iloc[0]["Distance"]

            if not valid_light_df.empty:
                latest_light = valid_light_df.iloc[0]["Light"]
            else:
                latest_light = lab_df.iloc[0]["Light"]

            latest_airflow = lab_df.iloc[0]["Airflow"]

            # Calculate statistics separately for each sensor from valid data only
            if valid_distance_df.empty:
                avg_distance = lab_df["Distance"].mean()
                max_distance = lab_df["Distance"].max()
                min_distance = lab_df["Distance"].min()
            else:
                avg_distance = valid_distance_df["Distance"].mean()
                max_distance = valid_distance_df["Distance"].max()
                min_distance = valid_distance_df["Distance"].min()

            if valid_light_df.empty:
                avg_light = lab_df["Light"].mean()
                max_light = lab_df["Light"].max()
                min_light = lab_df["Light"].min()
            else:
                avg_light = valid_light_df["Light"].mean()
                max_light = valid_light_df["Light"].max()
                min_light = valid_light_df["Light"].min()

            # Calculate sash opening metrics if calibration data available
            has_calibration = (lab_id, sublab_id) in FUMEHOOD_CALIBRATION
            percent_time_open = None
            hours_per_day_sash_open = None
            avg_sash_percent = None
            latest_sash_percent = None

            if has_calibration:
                # Calculate sash opening for all valid distance data points
                valid_df_copy = valid_distance_df.copy()
                valid_df_copy["SashPercentOpen"] = valid_df_copy["Distance"].apply(
                    lambda d: calculate_sash_percentage_open(d, lab_id, sublab_id)
                )
                valid_sash_df = valid_df_copy[valid_df_copy["SashPercentOpen"].notna()]

                # Find the most recent reading with valid sash percentage
                if not valid_sash_df.empty:
                    latest_sash_percent = valid_sash_df.iloc[0]["SashPercentOpen"]

                    # Calculate hours per day sash was open (> SASH_OPEN_THRESHOLD_PERCENT)
                    time_open = (
                        valid_sash_df["SashPercentOpen"] > SASH_OPEN_THRESHOLD_PERCENT
                    ).sum()
                    total_readings = len(valid_sash_df)
                    percent_time_open = (
                        (time_open / total_readings * 100) if total_readings > 0 else 0
                    )
                    # Convert percentage to hours per day
                    hours_per_day_sash_open = (percent_time_open / 100) * 24

                    # Calculate average sash opening based on points above threshold only
                    sash_above_threshold = valid_sash_df[
                        valid_sash_df["SashPercentOpen"] > SASH_OPEN_THRESHOLD_PERCENT
                    ]
                    avg_sash_percent = (
                        sash_above_threshold["SashPercentOpen"].mean()
                        if not sash_above_threshold.empty
                        else None
                    )

            # Calculate light on metrics if threshold data available
            has_light_threshold = (lab_id, sublab_id) in LIGHT_THRESHOLDS
            has_presence_threshold = False
            hours_per_week_lab_present = None
            hours_per_day_fumehood_light_on = None
            hours_per_day_fumehood_open_room_lights_off = None

            if has_light_threshold:
                has_presence_threshold = (
                    "room_light_on_threshold_lux"
                    in LIGHT_THRESHOLDS[(lab_id, sublab_id)]
                )

                if has_presence_threshold:
                    presence_readings = (
                        valid_light_df["Light"]
                        > LIGHT_THRESHOLDS[(lab_id, sublab_id)][
                            "room_light_on_threshold_lux"
                        ]
                    ).sum()
                    total_presence_readings = len(valid_light_df)
                    percent_time_present = (
                        (presence_readings / total_presence_readings * 100)
                        if total_presence_readings > 0
                        else 0
                    )
                    hours_per_week_lab_present = (percent_time_present / 100) * (24 * 7)

                # Calculate hours per day fumehood light was on
                light_on_readings = (
                    valid_light_df["Light"]
                    > LIGHT_THRESHOLDS[(lab_id, sublab_id)]["light_on_threshold_lux"]
                ).sum()
                total_light_readings = len(valid_light_df)
                percent_time_light_on = (
                    (light_on_readings / total_light_readings * 100)
                    if total_light_readings > 0
                    else 0
                )
                # Convert percentage to hours per day
                hours_per_day_fumehood_light_on = (percent_time_light_on / 100) * 24

                # Calculate hours per day fumehood is open AND room lights are off
                if (
                    has_calibration
                    and "valid_sash_df" in locals()
                    and not valid_sash_df.empty
                    and "room_light_on_threshold_lux"
                    in LIGHT_THRESHOLDS[(lab_id, sublab_id)]
                ):
                    # Merge sash data with light data
                    merged_df = valid_sash_df.copy()

                    # Fumehood open (sash > threshold) AND room lights off
                    fumehood_open_room_off_readings = (
                        (merged_df["SashPercentOpen"] > SASH_OPEN_THRESHOLD_PERCENT)
                        & (
                            merged_df["Light"]
                            <= LIGHT_THRESHOLDS[(lab_id, sublab_id)][
                                "room_light_on_threshold_lux"
                            ]
                        )
                    ).sum()

                    total_merged_readings = len(merged_df)
                    percent_time_open_room_off = (
                        (fumehood_open_room_off_readings / total_merged_readings * 100)
                        if total_merged_readings > 0
                        else 0
                    )
                    # Convert percentage to hours per day
                    hours_per_day_fumehood_open_room_lights_off = (
                        percent_time_open_room_off / 100
                    ) * 24

            html_lines += [
                '    <div class="lab-section">',
                f"      <h2>{get_display_label(lab_id, sublab_id)}</h2>",
                '      <div class="summary">',
                f"        <h3>Latest Reading ({latest_time.strftime('%Y-%m-%d %H:%M:%S')})</h3>",
            ]

            if has_calibration and latest_sash_percent is not None:
                html_lines.append(
                    f"        <p><strong>Sash Opening:</strong> {latest_sash_percent:.1f}% | "
                    f"<strong>Light Level:</strong> {latest_light:.2f} lux | "
                    f"<strong>Airflow:</strong> {latest_airflow:.2f} CFM</p>"
                )
            else:
                html_lines.append(
                    f"        <p><strong>Distance:</strong> {latest_distance:.2f} mm | "
                    f"<strong>Light Level:</strong> {latest_light:.2f} lux | "
                    f"<strong>Airflow:</strong> {latest_airflow:.2f} CFM</p>"
                )

            html_lines += [
                f"        <p><strong>Data Quality:</strong> {distance_errors} distance error(s) and {light_errors} light error(s) detected and excluded from analysis</p>",
                "      </div>",
                '      <div class="stats-grid">',
            ]

            # Add stat cards based on calibration availability
            if has_calibration and avg_sash_percent is not None:
                if has_presence_threshold and hours_per_week_lab_present is not None:
                    html_lines += [
                        '        <div class="stat-card light">',
                        "          <h3>Lab Presence</h3>",
                        f'          <div class="value">{hours_per_week_lab_present:.1f}</div>',
                        '          <div class="unit">hrs/week</div>',
                        "        </div>",
                    ]

                html_lines += [
                    '        <div class="stat-card distance">',
                    "          <h3>Avg Sash Opening</h3>",
                    f'          <div class="value">{avg_sash_percent:.1f}</div>',
                    '          <div class="unit">%</div>',
                    "        </div>",
                    '        <div class="stat-card light">',
                    "          <h3>Sash Open</h3>",
                    f'          <div class="value">{hours_per_day_sash_open:.1f}</div>',
                    '          <div class="unit">hrs/day</div>',
                    "        </div>",
                ]

                # Add light on metrics if available
                if has_light_threshold and hours_per_day_fumehood_light_on is not None:
                    html_lines += [
                        '        <div class="stat-card light">',
                        "          <h3>Fumehood Light On</h3>",
                        f'          <div class="value">{hours_per_day_fumehood_light_on:.1f}</div>',
                        '          <div class="unit">hrs/day</div>',
                        "        </div>",
                    ]

                # Add fumehood open + room lights off metric if available
                if hours_per_day_fumehood_open_room_lights_off is not None:
                    html_lines += [
                        '        <div class="stat-card light">',
                        "          <h3>Unattended Hood Open</h3>",
                        f'          <div class="value">{hours_per_day_fumehood_open_room_lights_off:.1f}</div>',
                        '          <div class="unit">hrs/day</div>',
                        "        </div>",
                    ]
            else:
                html_lines += [
                    '        <div class="stat-card distance">',
                    "          <h3>Current Distance</h3>",
                    f'          <div class="value">{latest_distance:.1f}</div>',
                    '          <div class="unit">mm</div>',
                    "        </div>",
                    '        <div class="stat-card distance">',
                    "          <h3>Avg Distance</h3>",
                    f'          <div class="value">{avg_distance:.1f}</div>',
                    '          <div class="unit">mm</div>',
                    "        </div>",
                ]

                if has_presence_threshold and hours_per_week_lab_present is not None:
                    html_lines += [
                        '        <div class="stat-card light">',
                        "          <h3>Lab Presence</h3>",
                        f'          <div class="value">{hours_per_week_lab_present:.1f}</div>',
                        '          <div class="unit">hrs/week</div>',
                        "        </div>",
                    ]
                else:
                    html_lines += [
                        '        <div class="stat-card light">',
                        "          <h3>Current Light</h3>",
                        f'          <div class="value">{latest_light:.1f}</div>',
                        '          <div class="unit">lux</div>',
                        "        </div>",
                    ]

                html_lines += [
                    '        <div class="stat-card light">',
                    "          <h3>Avg Light</h3>",
                    f'          <div class="value">{avg_light:.1f}</div>',
                    '          <div class="unit">lux</div>',
                    "        </div>",
                ]

                # Add light on metrics if available
                if has_light_threshold and hours_per_day_fumehood_light_on is not None:
                    html_lines += [
                        '        <div class="stat-card light">',
                        "          <h3>Fumehood Light On</h3>",
                        f'          <div class="value">{hours_per_day_fumehood_light_on:.1f}</div>',
                        '          <div class="unit">hrs/day</div>',
                        "        </div>",
                    ]

                # Add fumehood open + room lights off metric if available
                if hours_per_day_fumehood_open_room_lights_off is not None:
                    html_lines += [
                        '        <div class="stat-card light">',
                        "          <h3>Unattended Hood Open</h3>",
                        f'          <div class="value">{hours_per_day_fumehood_open_room_lights_off:.1f}</div>',
                        '          <div class="unit">hrs/day</div>',
                        "        </div>",
                    ]

            html_lines.append("      </div>")

            # Add plot if available
            if key in plot_files:
                html_lines.append(
                    f'      <img src="{plot_files[key]}" alt="{get_display_label(lab_id, sublab_id)} trends" />'
                )

            # Add data table (last 10 records)
            html_lines += [
                "      <h3>Recent History (Last 10 Records)</h3>",
                "      <table>",
                "        <thead>",
                "          <tr>",
                "            <th>Timestamp</th>",
                "            <th>Distance (mm)</th>",
                "            <th>Light (lux)</th>",
                "            <th>Airflow (CFM)</th>",
                "          </tr>",
                "        </thead>",
                "        <tbody>",
            ]

            for _, data_row in lab_df.head(10).iterrows():
                html_lines.append(
                    f"          <tr><td>{data_row['Timestamp'].strftime('%Y-%m-%d %H:%M:%S')}</td>"
                    f"<td>{data_row['Distance']:.2f}</td><td>{data_row['Light']:.2f}</td>"
                    f"<td>{data_row['Airflow']:.2f}</td></tr>"
                )

            html_lines += [
                "        </tbody>",
                "      </table>",
                "    </div>",
            ]

        html_lines += [
            "  </div>",
            "</body>",
            "</html>",
        ]

    # Write HTML file
    if out_file is None:
        out_file = plot_dir / "fumehood_dashboard.html"
    else:
        out_file = Path(out_file)

    with out_file.open("w", encoding="utf-8") as fh:
        fh.write("\n".join(html_lines))

    print(f"Dashboard created: {out_file}")
    return out_file


def main():
    plots_dir_default = os.getenv("PLOTS_DIR", "plots")
    parser = argparse.ArgumentParser(
        description="Generate Fumehood dashboard from SQL Server"
    )
    parser.add_argument(
        "--plot-dir",
        default=plots_dir_default,
        help=f"Directory for plots (default: {plots_dir_default})",
    )
    parser.add_argument(
        "--out", help="Output HTML file (default: plots/fumehood_dashboard.html)"
    )
    parser.add_argument(
        "--connection-string",
        help="Custom SQL connection string (optional)",
    )

    args = parser.parse_args()

    connection_string = args.connection_string or CONNECTION_STRING
    plot_dir = Path(args.plot_dir)

    print("Fetching data from SQL Server...")
    df = fetch_fumehood_data(connection_string)

    if df.empty:
        print("No data found in database. Please run Fumehood_sqlserver.py first.")
        return

    print(f"Found {len(df)} fumehood records")

    print("Creating plots...")
    plot_files = create_plots(df, plot_dir)

    print("Creating HTML dashboard...")
    out_file = Path(args.out) if args.out else None
    create_html_dashboard(df, plot_files, plot_dir, out_file)

    print("Done!")


if __name__ == "__main__":
    main()
