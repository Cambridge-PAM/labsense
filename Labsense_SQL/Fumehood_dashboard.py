"""Generate Fumehood dashboard from SQL Server data.

Queries the labsense SQL Server database for fumehood sensor data (distance and light)
grouped by laboratory and sublaboratory, and creates visualizations and an HTML dashboard.
"""

import sys
from pathlib import Path

# Add repository root to sys.path to allow absolute imports when running this file directly
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pyodbc
import pandas as pd
import argparse
from datetime import datetime
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

        # Create figure with two subplots (Distance and Light)
        fig, (ax1, ax2) = plt.subplots(
            2, 1, figsize=(12, 10), sharex=True, gridspec_kw={"hspace": 0.3}
        )

        # Plot Distance
        ax1.plot(
            lab_df["Timestamp"],
            lab_df["Distance"],
            marker="o",
            linestyle="-",
            color="#3498db",
            linewidth=2,
            markersize=4,
        )
        ax1.set_ylabel("Distance (mm)")
        ax1.set_title(f"Lab {lab_id} - Sublab {sublab_id} - Distance Over Time")
        ax1.grid(True, alpha=0.3)
        ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d %H:%M"))

        # Plot Light
        ax2.plot(
            lab_df["Timestamp"],
            lab_df["Light"],
            marker="s",
            linestyle="-",
            color="#e74c3c",
            linewidth=2,
            markersize=4,
        )
        ax2.set_xlabel("Date Time")
        ax2.set_ylabel("Light (lux)")
        ax2.set_title(f"Lab {lab_id} - Sublab {sublab_id} - Light Level Over Time")
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
        f"      <p>Real-time distance and light level monitoring • Generated {generated_time}</p>",
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

            # Get latest reading
            latest = lab_df.iloc[0]
            latest_distance = latest["Distance"]
            latest_light = latest["Light"]
            latest_airflow = latest["Airflow"]
            latest_time = latest["Timestamp"]

            # Calculate statistics
            avg_distance = lab_df["Distance"].mean()
            max_distance = lab_df["Distance"].max()
            min_distance = lab_df["Distance"].min()
            avg_light = lab_df["Light"].mean()
            max_light = lab_df["Light"].max()
            min_light = lab_df["Light"].min()

            html_lines += [
                '    <div class="lab-section">',
                f"      <h2>Lab {lab_id} - Sublab {sublab_id}</h2>",
                '      <div class="summary">',
                f"        <h3>Latest Reading ({latest_time.strftime('%Y-%m-%d %H:%M:%S')})</h3>",
                f"        <p><strong>Distance:</strong> {latest_distance:.2f} mm | "
                f"<strong>Light Level:</strong> {latest_light:.2f} lux | "
                f"<strong>Airflow:</strong> {latest_airflow:.2f} CFM</p>",
                "      </div>",
                '      <div class="stats-grid">',
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
                "      </div>",
            ]

            # Add plot if available
            if key in plot_files:
                html_lines.append(
                    f'      <img src="{plot_files[key]}" alt="Lab {lab_id} Sublab {sublab_id} trends" />'
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
    parser = argparse.ArgumentParser(
        description="Generate Fumehood dashboard from SQL Server"
    )
    parser.add_argument(
        "--plot-dir", default="plots", help="Directory for plots (default: plots)"
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
