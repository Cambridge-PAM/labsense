"""Generate Water dashboard from SQL Server data.

Queries the labsense SQL Server database for water sensor data
grouped by laboratory and sublaboratory (sinks), and creates visualizations and an HTML dashboard.
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Tuple

from dotenv import load_dotenv
import pandas as pd
import pyodbc

# Add repository root to sys.path to allow absolute imports when running this file directly
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
# pylint: disable=wrong-import-position

# Load environment variables from Labsense_SQL/.env
load_dotenv(Path(__file__).resolve().parent / ".env")
# pylint: enable=wrong-import-position

# SQL Server connection details (from Labsense_SQL/.env)
SQL_SERVER_NAME = os.getenv("SQL_SERVER", "MSM-FPM-70203\\LABSENSE")
DATABASE_NAME = os.getenv("SQL_DATABASE", "labsense")
TRUSTED_CONNECTION = os.getenv("SQL_TRUSTED_CONNECTION", "yes")
ENCRYPTION_PREF = os.getenv("SQL_ENCRYPTION", "Optional")

CONNECTION_STRING = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={SQL_SERVER_NAME};"
    f"DATABASE={DATABASE_NAME};"
    f"Trusted_Connection={TRUSTED_CONNECTION};"
    f"Encrypt={ENCRYPTION_PREF}"
)

# Lab ID to name mapping (same as Fumehood)
LAB_NAMES = {1: "Lab -1.025", 2: "Lab -1.041"}

# Sink naming configuration: {(lab_id, sublab_id): "Sink Name"}
SINK_NAMES = {
    (1, 1): "Sink 1",
    (1, 2): "Sink 2",
    (2, 1): "Sink 1",
    (2, 2): "Sink 2",
}

# Analysis window and validation constants
ANALYSIS_WINDOW_MONTHS = 2
INVALID_WATER_READING_L = 0.003
WATER_VALIDATION_TOLERANCE = 5e-4
OLYMPIC_POOL_VOLUME_L = 2_500_000


def get_analysis_start() -> pd.Timestamp:
    """Return the start timestamp for the analysis window (last N months)."""
    return pd.Timestamp(datetime.now()) - pd.DateOffset(months=ANALYSIS_WINDOW_MONTHS)


def get_lab_display_name(lab_id: int) -> str:
    """Get display name for a lab ID."""
    return LAB_NAMES.get(lab_id, f"Lab {lab_id}")


def get_sink_display_name(lab_id: int, sublab_id: int) -> str:
    """Get display name for a sink."""
    return SINK_NAMES.get((lab_id, sublab_id), f"Sink {sublab_id}")


def get_display_label(lab_id: int, sublab_id: int) -> str:
    """Get formatted display label for a lab/sublab combination."""
    lab_name = get_lab_display_name(lab_id)
    sink_name = get_sink_display_name(lab_id, sublab_id)
    return f"{sink_name} ({lab_name})"


def fetch_water_data(connection_string: str) -> pd.DataFrame:
    """Fetch all water data from SQL Server."""
    try:
        connection = pyodbc.connect(connection_string)
        query = """
            SELECT id, LabId, SublabId, Water, Timestamp
            FROM dbo.water
            ORDER BY Timestamp DESC
        """
        df = pd.read_sql(query, connection)
        connection.close()

        # Convert Timestamp to datetime
        df["Timestamp"] = pd.to_datetime(df["Timestamp"])

        return df
    except pyodbc.Error as ex:
        print(f"Error fetching water data: {ex}")
        return pd.DataFrame()


def identify_water_errors(df: pd.DataFrame) -> pd.Series:
    """Identify invalid water readings.

    Invalid readings are:
    - Negative values
    - Values equal to 0.003 L (within tolerance)
    """
    if df.empty or "Water" not in df.columns:
        return pd.Series([False] * len(df), index=df.index)

    is_negative = df["Water"] < 0
    is_invalid_point = (
        df["Water"] - INVALID_WATER_READING_L
    ).abs() <= WATER_VALIDATION_TOLERANCE
    return is_negative | is_invalid_point


def create_plots(df: pd.DataFrame, plot_dir: Path) -> Dict[Tuple[int, int], str]:
    """Create visualization plots for water consumption by lab/sublab."""
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

    # Filter to last two months
    analysis_start = get_analysis_start()
    df = df[df["Timestamp"] >= analysis_start]  # type: ignore[assignment]

    if df.empty:
        print(f"No data found in the last {ANALYSIS_WINDOW_MONTHS} months")
        return plot_files

    # Get unique lab/sublab combinations
    lab_sublab_combinations = df[["LabId", "SublabId"]].drop_duplicates()

    for _, row in lab_sublab_combinations.iterrows():
        lab_id = int(row["LabId"])
        sublab_id = int(row["SublabId"])
        key = (lab_id, sublab_id)

        # Filter data for this lab/sublab
        lab_df = df[
            (df["LabId"] == lab_id) & (df["SublabId"] == sublab_id)
        ].sort_values(
            by=["Timestamp"]
        )  # type: ignore[call-overload]

        if lab_df.empty:
            continue

        # Count and filter errors
        water_error_mask = identify_water_errors(lab_df)
        water_errors = water_error_mask.sum()
        plot_df = lab_df[~water_error_mask]

        if plot_df.empty:
            continue

        # Aggregate data into weekly intervals
        plot_df = (
            plot_df.set_index("Timestamp")
            .resample("W-MON", label="left", closed="left")["Water"]
            .sum()
            .reset_index()
        )

        # Create figure with water consumption plot
        fig, ax = plt.subplots(figsize=(12, 6))

        # Plot Water Consumption as bars (weekly)
        ax.bar(
            plot_df["Timestamp"],
            plot_df["Water"],
            color="#3498db",
            width=5.5,  # Weekly bar width (in days)
            edgecolor="#2980b9",
            linewidth=1.2,
        )
        ax.set_xlabel("Date")
        ax.set_ylabel("Weekly Water Consumption (L)")
        ax.set_title(
            f"{get_display_label(lab_id, sublab_id)}: Weekly Water Consumption ({water_errors} errors excluded)"
        )
        ax.grid(True, alpha=0.3, axis="y")
        ax.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=0, interval=1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
        ax.tick_params(axis="x", labelsize=8)

        fig.autofmt_xdate()
        plt.tight_layout()

        # Save plot
        plot_file = plot_dir / f"water_lab{lab_id}_sublab{sublab_id}.png"
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
    """Create an HTML dashboard for Water data."""

    plot_dir = Path(plot_dir)

    # Generate timestamp
    generated_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    html_lines = [
        "<!doctype html>",
        '<html lang="en">',
        "<head>",
        '  <meta charset="utf-8" />',
        "  <title>Water Consumption Dashboard</title>",
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
        "    .stat-card.water { background: linear-gradient(135deg, #3498db 0%, #2980b9 100%); }",
        "    .stat-card.consumption { background: linear-gradient(135deg, #1abc9c 0%, #16a085 100%); }",
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
        "      <h1>💧 Water Consumption Dashboard</h1>",
        f"      <p>Real-time water usage monitoring by sink • Generated {generated_time}</p>",
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
        all_df = df.copy()
        year_start = datetime(datetime.now().year, 1, 1)

        # Filter to last two months
        analysis_start = get_analysis_start()
        df = df[df["Timestamp"] >= analysis_start]  # type: ignore[assignment]

        if df.empty:
            html_lines += [
                f"    <p>No data found in the last {ANALYSIS_WINDOW_MONTHS} months.</p>",
                "  </div>",
                "</body>",
                "</html>",
            ]
        else:
            # Get unique lab/sublab combinations
            lab_sublab_combinations = (
                df[["LabId", "SublabId"]].drop_duplicates().sort_values(by=["LabId", "SublabId"])  # type: ignore[call-overload]
            )

            # Process each lab/sublab combination
            for _, row in lab_sublab_combinations.iterrows():
                lab_id = int(row["LabId"])
                sublab_id = int(row["SublabId"])
                key = (lab_id, sublab_id)

                # Filter data for this lab/sublab
                lab_df = df[
                    (df["LabId"] == lab_id) & (df["SublabId"] == sublab_id)
                ].sort_values(
                    by=["Timestamp"], ascending=False
                )  # type: ignore[call-overload]

                if lab_df.empty:
                    continue

                # Count and filter invalid points for statistics
                water_error_mask = identify_water_errors(lab_df)
                water_errors = water_error_mask.sum()
                valid_df = lab_df[~water_error_mask]

                # This-year data for this lab/sublab
                year_lab_df = all_df[
                    (all_df["LabId"] == lab_id)
                    & (all_df["SublabId"] == sublab_id)
                    & (all_df["Timestamp"] >= year_start)
                ].sort_values(
                    by=["Timestamp"], ascending=False
                )  # type: ignore[call-overload]

                year_water_error_mask = identify_water_errors(year_lab_df)
                year_valid_df = year_lab_df[~year_water_error_mask]

                # Get latest reading
                if valid_df.empty:
                    latest = lab_df.iloc[0]
                else:
                    latest = valid_df.iloc[0]

                latest_water = latest["Water"]
                latest_time = latest["Timestamp"]

                # Calculate statistics from valid data only
                if valid_df.empty:
                    max_water = lab_df["Water"].max()
                    total_water = lab_df["Water"].sum()
                else:
                    max_water = valid_df["Water"].max()
                    total_water = valid_df["Water"].sum()

                # Calculate this-year total and start date
                if year_valid_df.empty:
                    total_water_year = year_lab_df["Water"].sum()
                    total_start_date = year_lab_df["Timestamp"].min()
                else:
                    total_water_year = year_valid_df["Water"].sum()
                    total_start_date = year_valid_df["Timestamp"].min()

                if pd.isna(total_start_date):
                    start_date_label = "N/A"
                else:
                    start_date_label = pd.to_datetime(total_start_date).strftime(
                        "%Y-%m-%d"
                    )

                olympic_pool_equivalent = total_water_year / OLYMPIC_POOL_VOLUME_L

                # Calculate weekly average for the analysis window
                weekly_totals = (
                    valid_df.set_index("Timestamp")
                    .resample("W-MON", label="left", closed="left")["Water"]
                    .sum()
                )
                weekly_avg = weekly_totals.mean() if not weekly_totals.empty else 0

                html_lines += [
                    '    <div class="lab-section">',
                    f"      <h2>{get_display_label(lab_id, sublab_id)}</h2>",
                    '      <div class="summary">',
                    f"        <h3>Latest Reading ({latest_time.strftime('%Y-%m-%d %H:%M:%S')})</h3>",
                    f"        <p><strong>Water Consumption:</strong> {latest_water:.2f} L</p>",
                    f"        <p><strong>Data Quality:</strong> {water_errors} error(s) detected and excluded from analysis</p>",
                    "      </div>",
                    '      <div class="stats-grid">',
                    '        <div class="stat-card consumption">',
                    f"          <h3>Total (Last {ANALYSIS_WINDOW_MONTHS} Months)</h3>",
                    f'          <div class="value">{total_water:.1f}</div>',
                    '          <div class="unit">L</div>',
                    "        </div>",
                    '        <div class="stat-card consumption">',
                    "          <h3>Total (This Year)</h3>",
                    f'          <div class="value">{total_water_year:.1f}</div>',
                    f'          <div class="unit">L (since {start_date_label})</div>',
                    "        </div>",
                    '        <div class="stat-card consumption">',
                    "          <h3>Olympic Pools (This Year)</h3>",
                    f'          <div class="value">{olympic_pool_equivalent:.6f}</div>',
                    '          <div class="unit">pools (YTD)</div>',
                    "        </div>",
                    '        <div class="stat-card water">',
                    "          <h3>Weekly Average</h3>",
                    f'          <div class="value">{weekly_avg:.1f}</div>',
                    '          <div class="unit">L/week</div>',
                    "        </div>",
                    '        <div class="stat-card consumption">',
                    "          <h3>Peak Reading</h3>",
                    f'          <div class="value">{max_water:.1f}</div>',
                    '          <div class="unit">L</div>',
                    "        </div>",
                    "      </div>",
                ]

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
                    "            <th>Water Consumption (L)</th>",
                    "          </tr>",
                    "        </thead>",
                    "        <tbody>",
                ]

                for _, data_row in lab_df.head(10).iterrows():
                    html_lines.append(
                        f"          <tr><td>{data_row['Timestamp'].strftime('%Y-%m-%d %H:%M:%S')}</td>"
                        f"<td>{data_row['Water']:.2f}</td></tr>"
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
        out_file = plot_dir / "water_dashboard.html"
    else:
        out_file = Path(out_file)

    with out_file.open("w", encoding="utf-8") as fh:
        fh.write("\n".join(html_lines))

    print(f"Dashboard created: {out_file}")
    return out_file


def main():
    plots_dir_default = os.getenv("PLOTS_DIR", "plots")
    parser = argparse.ArgumentParser(
        description="Generate Water dashboard from SQL Server"
    )
    parser.add_argument(
        "--plot-dir",
        default=plots_dir_default,
        help=f"Directory for plots (default: {plots_dir_default})",
    )
    parser.add_argument(
        "--out", help="Output HTML file (default: plots/water_dashboard.html)"
    )
    parser.add_argument(
        "--connection-string",
        help="Custom SQL connection string (optional)",
    )

    args = parser.parse_args()

    connection_string = args.connection_string or CONNECTION_STRING
    plot_dir = Path(args.plot_dir)

    print("Fetching data from SQL Server...")
    df = fetch_water_data(connection_string)

    if df.empty:
        print("No data found in database.")
        return

    print(f"Found {len(df)} water records")

    print("Creating plots...")
    plot_files = create_plots(df, plot_dir)

    print("Creating HTML dashboard...")
    out_file = Path(args.out) if args.out else None
    create_html_dashboard(df, plot_files, plot_dir, out_file)

    print("Done!")


if __name__ == "__main__":
    main()
