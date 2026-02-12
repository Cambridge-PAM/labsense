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

# Fumehood calibration data: {(lab_id, sublab_id): {"fully_closed_mm": mm, "fully_open_mm": mm}}
# Distance values for sash fully closed (0% open) and fully open (100% open)
FUMEHOOD_CALIBRATION = {
    (1, 3): {"fully_closed_mm": 760, "fully_open_mm": 100},
}

# Light threshold configuration: {(lab_id, sublab_id): {"light_on_threshold_lux": lux, "room_light_on_threshold_lux": lux}}
# Light level above light_on_threshold indicates the fumehood light is on
# Light level above room_light_on_threshold indicates the room lights are on
LIGHT_THRESHOLDS = {
    (1, 3): {"light_on_threshold_lux": 80, "room_light_on_threshold_lux": 1},
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

        # Count errors (negative values)
        distance_errors = (lab_df["Distance"] < 0).sum()
        light_errors = (lab_df["Light"] < 0).sum()
        total_errors = distance_errors + light_errors

        # Filter to valid data only (non-negative values)
        plot_df = lab_df[(lab_df["Distance"] >= 0) & (lab_df["Light"] >= 0)]

        if plot_df.empty:
            continue

        # Check if we have calibration data for this fumehood
        has_calibration = (lab_id, sublab_id) in FUMEHOOD_CALIBRATION

        if has_calibration:
            # Calculate sash opening percentage for each point
            plot_df = plot_df.copy()
            plot_df["SashPercentOpen"] = plot_df["Distance"].apply(
                lambda d: calculate_sash_percentage_open(d, lab_id, sublab_id)
            )
            # Filter to valid percentage values only (0-100)
            plot_df = plot_df[plot_df["SashPercentOpen"].notna()]

            if plot_df.empty:
                continue

        # Create figure with two subplots (Sash % / Distance and Light)
        fig, (ax1, ax2) = plt.subplots(
            2, 1, figsize=(12, 10), sharex=True, gridspec_kw={"hspace": 0.3}
        )

        # Plot Sash Opening Percentage or Distance
        if has_calibration:
            ax1.plot(
                plot_df["Timestamp"],
                plot_df["SashPercentOpen"],
                marker="o",
                linestyle="-",
                color="#3498db",
                linewidth=2,
                markersize=4,
            )
            ax1.set_ylabel("Sash Opening (%)")
            ax1.set_ylim(0, 100)
            sash_error_count = (lab_df["Distance"] < 0).sum()
            ax1.set_title(
                f"{get_display_label(lab_id, sublab_id)}: Sash Opening ({total_errors} errors excluded)"
            )
        else:
            ax1.plot(
                plot_df["Timestamp"],
                plot_df["Distance"],
                marker="o",
                linestyle="-",
                color="#3498db",
                linewidth=2,
                markersize=4,
            )
            ax1.set_ylabel("Distance (mm)")
            ax1.set_title(
                f"{get_display_label(lab_id, sublab_id)}: Sash Raw Distance ({total_errors} errors excluded)"
            )
        ax1.grid(True, alpha=0.3)
        ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d %H:%M"))

        # Plot Light
        ax2.plot(
            plot_df["Timestamp"],
            plot_df["Light"],
            marker="s",
            linestyle="-",
            color="#e74c3c",
            linewidth=2,
            markersize=4,
        )
        ax2.set_xlabel("Date Time")
        ax2.set_ylabel("Light (lux)")
        ax2.set_title(
            f"{get_display_label(lab_id, sublab_id)}: Light Level ({total_errors} errors excluded)"
        )
        ax2.grid(True, alpha=0.3)
        ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d %H:%M"))

        fig.autofmt_xdate()
        plt.tight_layout()

        # Save plot
        plot_file = plot_dir / f"fumehood_lab{lab_id}_sublab{sublab_id}.png"
        plt.savefig(plot_file, dpi=150, bbox_inches="tight")
        plt.close()

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

            # Count errors (negative values)
            distance_errors = (lab_df["Distance"] < 0).sum()
            light_errors = (lab_df["Light"] < 0).sum()
            total_errors = distance_errors + light_errors

            # Filter to valid data only (non-negative values) for statistics
            valid_df = lab_df[(lab_df["Distance"] >= 0) & (lab_df["Light"] >= 0)]

            # Get latest valid reading
            if valid_df.empty:
                latest = lab_df.iloc[0]
            else:
                latest = valid_df.iloc[0]

            latest_distance = latest["Distance"]
            latest_light = latest["Light"]
            latest_airflow = latest["Airflow"]
            latest_time = latest["Timestamp"]

            # Calculate statistics from valid data only
            if valid_df.empty:
                avg_distance = lab_df["Distance"].mean()
                max_distance = lab_df["Distance"].max()
                min_distance = lab_df["Distance"].min()
                avg_light = lab_df["Light"].mean()
                max_light = lab_df["Light"].max()
                min_light = lab_df["Light"].min()
            else:
                avg_distance = valid_df["Distance"].mean()
                max_distance = valid_df["Distance"].max()
                min_distance = valid_df["Distance"].min()
                avg_light = valid_df["Light"].mean()
                max_light = valid_df["Light"].max()
                min_light = valid_df["Light"].min()

            # Calculate sash opening metrics if calibration data available
            has_calibration = (lab_id, sublab_id) in FUMEHOOD_CALIBRATION
            percent_time_open = None
            hours_per_day_sash_open = None
            avg_sash_percent = None
            latest_sash_percent = None

            if has_calibration:
                # Calculate sash opening for all valid data points
                valid_df_copy = valid_df.copy()
                valid_df_copy["SashPercentOpen"] = valid_df_copy["Distance"].apply(
                    lambda d: calculate_sash_percentage_open(d, lab_id, sublab_id)
                )
                valid_sash_df = valid_df_copy[valid_df_copy["SashPercentOpen"].notna()]

                # Find the most recent reading with valid sash percentage
                if not valid_sash_df.empty:
                    latest_sash_percent = valid_sash_df.iloc[0]["SashPercentOpen"]

                    # Calculate hours per day sash was open (>= 25% open threshold)
                    time_open = (valid_sash_df["SashPercentOpen"] >= 25).sum()
                    total_readings = len(valid_sash_df)
                    percent_time_open = (
                        (time_open / total_readings * 100) if total_readings > 0 else 0
                    )
                    # Convert percentage to hours per day
                    hours_per_day_sash_open = (percent_time_open / 100) * 24
                    avg_sash_percent = valid_sash_df["SashPercentOpen"].mean()

            # Calculate light on metrics if threshold data available
            has_light_threshold = (lab_id, sublab_id) in LIGHT_THRESHOLDS
            hours_per_day_fumehood_light_on = None
            hours_per_day_room_light_on = None

            if has_light_threshold:
                # Calculate hours per day fumehood light was on
                light_on_readings = (
                    valid_df["Light"]
                    > LIGHT_THRESHOLDS[(lab_id, sublab_id)]["light_on_threshold_lux"]
                ).sum()
                total_light_readings = len(valid_df)
                percent_time_light_on = (
                    (light_on_readings / total_light_readings * 100)
                    if total_light_readings > 0
                    else 0
                )
                # Convert percentage to hours per day
                hours_per_day_fumehood_light_on = (percent_time_light_on / 100) * 24

                # Calculate hours per day room lights were on (if threshold defined)
                if (
                    "room_light_on_threshold_lux"
                    in LIGHT_THRESHOLDS[(lab_id, sublab_id)]
                ):
                    room_light_on_readings = (
                        valid_df["Light"]
                        > LIGHT_THRESHOLDS[(lab_id, sublab_id)][
                            "room_light_on_threshold_lux"
                        ]
                    ).sum()
                    percent_time_room_light_on = (
                        (room_light_on_readings / total_light_readings * 100)
                        if total_light_readings > 0
                        else 0
                    )
                    # Convert percentage to hours per day
                    hours_per_day_room_light_on = (
                        percent_time_room_light_on / 100
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
                f"        <p><strong>Data Quality:</strong> {total_errors} error(s) detected and excluded from analysis</p>",
                "      </div>",
                '      <div class="stats-grid">',
            ]

            # Add stat cards based on calibration availability
            if has_calibration and avg_sash_percent is not None:
                # Use calculated latest_sash_percent or fall back to average
                display_sash_percent = (
                    latest_sash_percent
                    if latest_sash_percent is not None
                    else avg_sash_percent
                )

                html_lines += [
                    '        <div class="stat-card distance">',
                    "          <h3>Current Sash Opening</h3>",
                    f'          <div class="value">{display_sash_percent:.1f}</div>',
                    '          <div class="unit">%</div>',
                    "        </div>",
                    '        <div class="stat-card light">',
                    "          <h3>Current Light</h3>",
                    f'          <div class="value">{latest_light:.1f}</div>',
                    '          <div class="unit">lux</div>',
                    "        </div>",
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

                # Add room light on metric if available
                if hours_per_day_room_light_on is not None:
                    html_lines += [
                        '        <div class="stat-card light">',
                        "          <h3>Room Lights On</h3>",
                        f'          <div class="value">{hours_per_day_room_light_on:.1f}</div>',
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
                    '        <div class="stat-card light">',
                    "          <h3>Current Light</h3>",
                    f'          <div class="value">{latest_light:.1f}</div>',
                    '          <div class="unit">lux</div>',
                    "        </div>",
                    '        <div class="stat-card distance">',
                    "          <h3>Avg Distance</h3>",
                    f'          <div class="value">{avg_distance:.1f}</div>',
                    '          <div class="unit">mm</div>',
                    "        </div>",
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

                # Add room light on metric if available
                if hours_per_day_room_light_on is not None:
                    html_lines += [
                        '        <div class="stat-card light">',
                        "          <h3>Room Lights On</h3>",
                        f'          <div class="value">{hours_per_day_room_light_on:.1f}</div>',
                        '          <div class="unit">hrs/day</div>',
                        "        </div>",
                    ]

            html_lines.append("      </div>")

            # Add plot if available
            if key in plot_files:
                html_lines.append(
                    f'      <img src="{plot_files[key]}" alt="{get_display_label(lab_id, sublab_id)} trends" />'
                )

            # Add data table (last 20 records)
            html_lines += [
                "      <h3>Recent History (Last 20 Records)</h3>",
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

            for _, data_row in lab_df.head(20).iterrows():
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
