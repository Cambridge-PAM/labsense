"""Generate SEN66 dashboard from SQL Server data.

Queries the labsense SQL Server database for SEN66 sensor data over the last
week and creates visualizations and an HTML dashboard.
"""

import argparse
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd
import pyodbc
from dotenv import load_dotenv

# Load environment variables from Labsense_SQL/.env
load_dotenv(Path(__file__).resolve().parent / ".env")

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

LAB_NAMES = {1: "PAM Group", 2: "Other Group"}


def get_lab_display_name(lab_id: int) -> str:
    """Get display name for a lab ID."""
    return LAB_NAMES.get(lab_id, f"Lab {lab_id}")


def get_display_label(lab_id: int, sublab_id: int) -> str:
    """Get formatted display label for a lab/sublab combination."""
    return f"SEN66 Sensor {sublab_id} ({get_lab_display_name(lab_id)})"


def fetch_sen66_data(connection_string: str, days: int = 7) -> pd.DataFrame:
    """Fetch SEN66 data for the last N days from SQL Server."""
    try:
        connection = pyodbc.connect(connection_string)
        query = """
            SELECT id, LabId, SublabId, Temperature, Humidity, Co2, Voc, Nox, Pm1, Pm25, Pm4, Pm10, Timestamp
            FROM dbo.sen66
            WHERE Timestamp >= DATEADD(day, -?, GETDATE())
            ORDER BY Timestamp ASC
        """
        df = pd.read_sql(query, connection, params=[days])
        connection.close()

        if not df.empty:
            df["Timestamp"] = pd.to_datetime(df["Timestamp"])

        return df
    except pyodbc.Error as ex:
        print(f"Error fetching SEN66 data: {ex}")
        return pd.DataFrame()


def create_plots(
    df: pd.DataFrame, plot_dir: Path
) -> Dict[Tuple[int, int], Dict[str, str]]:
    """Create requested SEN66 plots for each lab/sublab combination."""
    try:
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
    except ImportError:
        print("matplotlib not available - skipping plots")
        return {}

    plt.rcParams["pdf.fonttype"] = 42
    plt.rcParams["ps.fonttype"] = 42

    plot_dir = Path(plot_dir)
    plot_dir.mkdir(parents=True, exist_ok=True)
    plot_files: Dict[Tuple[int, int], Dict[str, str]] = {}

    if df.empty:
        return plot_files

    combinations = df[["LabId", "SublabId"]].drop_duplicates()

    for _, row in combinations.iterrows():
        lab_id = int(row["LabId"])
        sublab_id = int(row["SublabId"])
        key = (lab_id, sublab_id)

        group_df = df[(df["LabId"] == lab_id) & (df["SublabId"] == sublab_id)].copy()
        group_df = group_df.sort_values(by=["Timestamp"])

        if group_df.empty:
            continue

        label = get_display_label(lab_id, sublab_id)
        files_for_sensor: Dict[str, str] = {}

        # 1) Temperature + Relative Humidity with secondary axis
        fig, ax1 = plt.subplots(figsize=(12, 5))
        ax2 = ax1.twinx()

        ax1.plot(
            group_df["Timestamp"],
            group_df["Temperature"],
            color="#e74c3c",
            linewidth=1.5,
            label="Temperature",
        )
        ax2.plot(
            group_df["Timestamp"],
            group_df["Humidity"],
            color="#3498db",
            linewidth=1.5,
            label="Relative Humidity",
        )

        ax1.set_title(f"{label}: Temperature and Relative Humidity (last 7 days)")
        ax1.set_xlabel("Time")
        ax1.set_ylabel("Temperature (C)", color="#e74c3c")
        ax2.set_ylabel("Relative Humidity (%RH)", color="#3498db")
        ax1.grid(True, alpha=0.3)
        ax1.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
        plt.setp(ax1.get_xticklabels(), rotation=45, ha="right")

        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper right")

        plt.tight_layout()
        temp_rh_file = plot_dir / f"sen66_lab{lab_id}_sublab{sublab_id}_temp_rh.png"
        plt.savefig(temp_rh_file, dpi=150, bbox_inches="tight")
        plt.savefig(temp_rh_file.with_suffix(".pdf"), bbox_inches="tight")
        plt.close()
        files_for_sensor["temp_rh"] = temp_rh_file.name

        # 2) PM1 / PM2.5 / PM4 / PM10 on one axis
        fig, ax = plt.subplots(figsize=(12, 5))
        ax.plot(group_df["Timestamp"], group_df["Pm1"], linewidth=1.3, label="PM1")
        ax.plot(group_df["Timestamp"], group_df["Pm25"], linewidth=1.3, label="PM2.5")
        ax.plot(group_df["Timestamp"], group_df["Pm4"], linewidth=1.3, label="PM4")
        ax.plot(group_df["Timestamp"], group_df["Pm10"], linewidth=1.3, label="PM10")

        ax.set_title(f"{label}: Particulate Matter (last 7 days)")
        ax.set_xlabel("Time")
        ax.set_ylabel("Concentration (ug/m^3)")
        ax.grid(True, alpha=0.3)
        ax.legend(loc="upper right")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
        plt.setp(ax.get_xticklabels(), rotation=45, ha="right")

        plt.tight_layout()
        pm_file = plot_dir / f"sen66_lab{lab_id}_sublab{sublab_id}_pm.png"
        plt.savefig(pm_file, dpi=150, bbox_inches="tight")
        plt.savefig(pm_file.with_suffix(".pdf"), bbox_inches="tight")
        plt.close()
        files_for_sensor["pm"] = pm_file.name

        # 3) CO2 with ppm axis
        fig, ax = plt.subplots(figsize=(12, 5))
        ax.plot(group_df["Timestamp"], group_df["Co2"], color="#2ecc71", linewidth=1.5)
        ax.set_title(f"{label}: CO2 (last 7 days)")
        ax.set_xlabel("Time")
        ax.set_ylabel("CO2 (ppm)")
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
        plt.setp(ax.get_xticklabels(), rotation=45, ha="right")

        plt.tight_layout()
        co2_file = plot_dir / f"sen66_lab{lab_id}_sublab{sublab_id}_co2.png"
        plt.savefig(co2_file, dpi=150, bbox_inches="tight")
        plt.savefig(co2_file.with_suffix(".pdf"), bbox_inches="tight")
        plt.close()
        files_for_sensor["co2"] = co2_file.name

        # 4) VOC + NOx with index axis fixed to 0-500
        fig, ax = plt.subplots(figsize=(12, 5))
        ax.plot(
            group_df["Timestamp"],
            group_df["Voc"],
            color="#9b59b6",
            linewidth=1.5,
            label="VOC",
        )
        ax.plot(
            group_df["Timestamp"],
            group_df["Nox"],
            color="#e67e22",
            linewidth=1.5,
            label="NOx",
        )
        ax.set_title(f"{label}: VOC and NOx Index (last 7 days)")
        ax.set_xlabel("Time")
        ax.set_ylabel("Index")
        ax.set_ylim(0, 500)
        ax.grid(True, alpha=0.3)
        ax.legend(loc="upper right")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
        plt.setp(ax.get_xticklabels(), rotation=45, ha="right")

        plt.tight_layout()
        voc_nox_file = plot_dir / f"sen66_lab{lab_id}_sublab{sublab_id}_voc_nox.png"
        plt.savefig(voc_nox_file, dpi=150, bbox_inches="tight")
        plt.savefig(voc_nox_file.with_suffix(".pdf"), bbox_inches="tight")
        plt.close()
        files_for_sensor["voc_nox"] = voc_nox_file.name

        plot_files[key] = files_for_sensor

    return plot_files


def create_html_dashboard(
    df: pd.DataFrame,
    plot_files: Dict[Tuple[int, int], Dict[str, str]],
    plot_dir: Path,
    out_file: Optional[Path] = None,
) -> Path:
    """Create an HTML dashboard for SEN66 data."""
    generated_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    plot_dir = Path(plot_dir)

    html_lines = [
        "<!doctype html>",
        '<html lang="en">',
        "<head>",
        '  <meta charset="utf-8" />',
        "  <title>SEN66 Dashboard</title>",
        "  <style>",
        "    body { font-family: Arial, Helvetica, sans-serif; margin: 20px; background: #f5f5f5; }",
        "    .container { max-width: 1400px; margin: 0 auto; }",
        "    .header { background: white; border-radius: 10px; padding: 25px; margin-bottom: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }",
        "    .section { background: white; border-radius: 10px; padding: 25px; margin-bottom: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }",
        "    h1, h2, h3 { color: #2c3e50; }",
        "    img { max-width: 100%; height: auto; border-radius: 8px; margin: 10px 0 20px 0; border: 1px solid #e0e0e0; }",
        "    .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 10px; margin-bottom: 16px; }",
        "    .stat { background: #f8fbff; border: 1px solid #d9e7f5; border-radius: 8px; padding: 10px; }",
        "    .label { color: #7f8c8d; font-size: 0.9em; }",
        "    .value { color: #2c3e50; font-size: 1.2em; font-weight: 600; }",
        "  </style>",
        "</head>",
        "<body>",
        '  <div class="container">',
        '    <div class="header">',
        "      <h1>SEN66 Dashboard</h1>",
        "      <p>Past-week trends for temperature, humidity, PM, CO2, VOC, and NOx.</p>",
        f"      <p><strong>Generated:</strong> {generated_time}</p>",
        f"      <p><strong>Total data points:</strong> {len(df)}</p>",
        "    </div>",
    ]

    if df.empty:
        html_lines.extend(
            [
                '    <div class="section">',
                "      <h2>No data found</h2>",
                "    </div>",
            ]
        )

    for (lab_id, sublab_id), files in sorted(plot_files.items()):
        label = get_display_label(lab_id, sublab_id)
        sensor_df = df[(df["LabId"] == lab_id) & (df["SublabId"] == sublab_id)].copy()

        if sensor_df.empty:
            continue

        latest = sensor_df.sort_values(by=["Timestamp"]).iloc[-1]

        html_lines.extend(
            [
                '    <div class="section">',
                f"      <h2>{label}</h2>",
                '      <div class="stats">',
                f'        <div class="stat"><div class="label">Latest Timestamp</div><div class="value">{latest["Timestamp"]}</div></div>',
                f'        <div class="stat"><div class="label">Temperature (C)</div><div class="value">{latest["Temperature"]:.2f}</div></div>',
                f'        <div class="stat"><div class="label">Humidity (%RH)</div><div class="value">{latest["Humidity"]:.2f}</div></div>',
                f'        <div class="stat"><div class="label">CO2 (ppm)</div><div class="value">{latest["Co2"]:.0f}</div></div>',
                f'        <div class="stat"><div class="label">VOC Index</div><div class="value">{latest["Voc"]:.1f}</div></div>',
                f'        <div class="stat"><div class="label">NOx Index</div><div class="value">{latest["Nox"]:.1f}</div></div>',
                "      </div>",
                "      <h3>Temperature and Relative Humidity</h3>",
                f'      <img src="{files["temp_rh"]}" alt="Temperature and humidity plot" />',
                "      <h3>PM1 / PM2.5 / PM4 / PM10</h3>",
                f'      <img src="{files["pm"]}" alt="Particulate matter plot" />',
                "      <h3>CO2</h3>",
                f'      <img src="{files["co2"]}" alt="CO2 plot" />',
                "      <h3>VOC and NOx Index</h3>",
                f'      <img src="{files["voc_nox"]}" alt="VOC and NOx plot" />',
                "    </div>",
            ]
        )

    html_lines.extend(
        [
            "  </div>",
            "</body>",
            "</html>",
        ]
    )

    output_path = out_file or (plot_dir / "sen66_dashboard.html")
    output_path = Path(output_path)
    output_path.write_text("\n".join(html_lines), encoding="utf-8")
    return output_path


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate SEN66 dashboard from SQL data"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of trailing days to include (default: 7)",
    )
    parser.add_argument(
        "--plot-dir",
        default=str(Path(__file__).resolve().parents[1] / "plots" / "sen66"),
        help="Directory to write plot images and dashboard HTML",
    )
    parser.add_argument(
        "--out-file",
        default=None,
        help="Optional output HTML path (default: <plot-dir>/sen66_dashboard.html)",
    )
    return parser.parse_args()


def main() -> None:
    """Generate SEN66 plots and dashboard HTML."""
    args = parse_args()
    plot_dir = Path(args.plot_dir)
    out_file = Path(args.out_file) if args.out_file else None

    df = fetch_sen66_data(CONNECTION_STRING, days=args.days)
    if df.empty:
        print("No SEN66 data found for selected time range.")

    plot_files = create_plots(df, plot_dir)
    output_html = create_html_dashboard(df, plot_files, plot_dir, out_file=out_file)

    print(f"Generated SEN66 dashboard: {output_html}")
    print(f"Generated plot groups: {len(plot_files)}")


if __name__ == "__main__":
    main()
