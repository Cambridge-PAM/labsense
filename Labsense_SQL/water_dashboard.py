"""Generate Water dashboard from SQL Server data.

Queries the labsense SQL Server database for water sensor data
grouped by laboratory and sublaboratory (sinks), and creates visualizations and an HTML dashboard.
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

# Lab ID to name mapping (same as Fumehood)
LAB_NAMES = {1: "Lab -1.025", 2: "Lab -1.041"}

# Sink naming configuration: {(lab_id, sublab_id): "Sink Name"}
SINK_NAMES = {
    (1, 1): "Sink 1",
    (1, 2): "Sink 2",
    (2, 1): "Sink 1",
    (2, 2): "Sink 2",
}


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

    # Filter to last 7 days
    last_week = datetime.now() - timedelta(days=7)
    df = df[df["Timestamp"] >= last_week]  # type: ignore[assignment]

    if df.empty:
        print("No data found in the last 7 days")
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

        # Count errors (negative values)
        water_errors = (lab_df["Water"] < 0).sum()

        # Filter to valid data only (non-negative values)
        plot_df = lab_df[lab_df["Water"] >= 0]

        if plot_df.empty:
            continue

        # Aggregate data into 30-minute intervals
        plot_df = (
            plot_df.set_index("Timestamp")
            .resample("30min")["Water"]
            .sum()
            .reset_index()
        )

        # Create figure with water consumption plot
        fig, ax = plt.subplots(figsize=(12, 6))

        # Plot Water Consumption as bars
        ax.bar(
            plot_df["Timestamp"],
            plot_df["Water"],
            color="#3498db",
            width=0.02,  # Adjust bar width for better visibility
            edgecolor="none",
        )
        ax.set_xlabel("Date Time")
        ax.set_ylabel("Water Consumption (L)")
        ax.set_title(
            f"{get_display_label(lab_id, sublab_id)}: Water Consumption ({water_errors} errors excluded)"
        )
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d %H:%M"))

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

                # Count errors (negative values)
                water_errors = (lab_df["Water"] < 0).sum()

                # Filter to valid data only (non-negative values) for statistics
                valid_df = lab_df[lab_df["Water"] >= 0]

                # Get latest reading
                if valid_df.empty:
                    latest = lab_df.iloc[0]
                else:
                    latest = valid_df.iloc[0]

                latest_water = latest["Water"]
                latest_time = latest["Timestamp"]

                # Calculate statistics from valid data only
                if valid_df.empty:
                    avg_water = lab_df["Water"].mean()
                    max_water = lab_df["Water"].max()
                    min_water = lab_df["Water"].min()
                    total_water = lab_df["Water"].sum()
                else:
                    avg_water = valid_df["Water"].mean()
                    max_water = valid_df["Water"].max()
                    min_water = valid_df["Water"].min()
                    total_water = valid_df["Water"].sum()

                # Calculate daily average (last 7 days)
                days_of_data = (datetime.now() - last_week).days
                daily_avg = total_water / days_of_data if days_of_data > 0 else 0

                html_lines += [
                    '    <div class="lab-section">',
                    f"      <h2>{get_display_label(lab_id, sublab_id)}</h2>",
                    '      <div class="summary">',
                    f"        <h3>Latest Reading ({latest_time.strftime('%Y-%m-%d %H:%M:%S')})</h3>",
                    f"        <p><strong>Water Consumption:</strong> {latest_water:.2f} L</p>",
                    f"        <p><strong>Data Quality:</strong> {water_errors} error(s) detected and excluded from analysis</p>",
                    "      </div>",
                    '      <div class="stats-grid">',
                    '        <div class="stat-card water">',
                    "          <h3>Current Reading</h3>",
                    f'          <div class="value">{latest_water:.1f}</div>',
                    '          <div class="unit">L</div>',
                    "        </div>",
                    '        <div class="stat-card consumption">',
                    "          <h3>Total (7 days)</h3>",
                    f'          <div class="value">{total_water:.1f}</div>',
                    '          <div class="unit">L</div>',
                    "        </div>",
                    '        <div class="stat-card water">',
                    "          <h3>Daily Average</h3>",
                    f'          <div class="value">{daily_avg:.1f}</div>',
                    '          <div class="unit">L/day</div>',
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

                # Add data table (last 20 records)
                html_lines += [
                    "      <h3>Recent History (Last 20 Records)</h3>",
                    "      <table>",
                    "        <thead>",
                    "          <tr>",
                    "            <th>Timestamp</th>",
                    "            <th>Water Consumption (L)</th>",
                    "          </tr>",
                    "        </thead>",
                    "        <tbody>",
                ]

                for _, data_row in lab_df.head(20).iterrows():
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
