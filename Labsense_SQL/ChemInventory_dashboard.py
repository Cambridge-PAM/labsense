"""Generate ChemInventory dashboard from SQL Server data.

Queries the labsense SQL Server database for chemical inventory data across
different environmental categories (Composite, Incineration, VOC, Aquatic, Air, Health)
and creates visualizations and an HTML dashboard.
"""

import sys
from pathlib import Path

# Add repository root to sys.path to allow absolute imports when running this file directly
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pyodbc
import pandas as pd
import argparse
from datetime import datetime
from typing import Optional, Dict
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

# Categories to query
CATEGORIES = [
    "chemComposite",
    "chemIncineration",
    "chemVOC",
    "chemAquatic",
    "chemAir",
    "chemHealth",
]

CATEGORY_DISPLAY_NAMES = {
    "chemComposite": "Composite Environmental Impact",
    "chemIncineration": "Incineration",
    "chemVOC": "Volatile Organic Compounds (VOC)",
    "chemAquatic": "Aquatic Toxicity",
    "chemAir": "Air Emissions",
    "chemHealth": "Health Hazards",
}


def fetch_category_data(category: str, connection_string: str) -> pd.DataFrame:
    """Fetch data for a specific category from SQL Server."""
    try:
        connection = pyodbc.connect(connection_string)
        query = f"""
            SELECT id, RedVol, YellowVol, GreenVol, Timestamp
            FROM [{category}]
            ORDER BY Timestamp DESC
        """
        df = pd.read_sql(query, connection)
        connection.close()
        return df
    except pyodbc.Error as ex:
        print(f"Error fetching {category}: {ex}")
        return pd.DataFrame()


def fetch_all_data(connection_string: str) -> Dict[str, pd.DataFrame]:
    """Fetch data for all categories."""
    data = {}
    for category in CATEGORIES:
        df = fetch_category_data(category, connection_string)
        if not df.empty:
            data[category] = df
    return data


def create_plots(data: Dict[str, pd.DataFrame], plot_dir: Path):
    """Create visualization plots for the data."""
    try:
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
    except ImportError:
        print("matplotlib not available - skipping plots")
        return {}

    plot_dir = Path(plot_dir)
    plot_dir.mkdir(parents=True, exist_ok=True)
    plot_files = {}

    for category, df in data.items():
        if df.empty:
            continue

        display_name = CATEGORY_DISPLAY_NAMES.get(category, category)

        # Convert Timestamp to datetime
        df["Timestamp"] = pd.to_datetime(df["Timestamp"])

        # Create figure with two subplots
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))

        # Plot 1: Stacked area chart
        ax1.fill_between(
            df["Timestamp"], 0, df["RedVol"], label="Red", color="#e74c3c", alpha=0.7
        )
        ax1.fill_between(
            df["Timestamp"],
            df["RedVol"],
            df["RedVol"] + df["YellowVol"],
            label="Yellow",
            color="#f39c12",
            alpha=0.7,
        )
        ax1.fill_between(
            df["Timestamp"],
            df["RedVol"] + df["YellowVol"],
            df["RedVol"] + df["YellowVol"] + df["GreenVol"],
            label="Green",
            color="#27ae60",
            alpha=0.7,
        )
        ax1.set_xlabel("Date")
        ax1.set_ylabel("Volume (L)")
        ax1.set_title(f"{display_name} - Volume by Category (Stacked)")
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
        fig.autofmt_xdate()

        # Plot 2: Individual lines
        ax2.plot(df["Timestamp"], df["RedVol"], "o-", label="Red", color="#e74c3c")
        ax2.plot(
            df["Timestamp"], df["YellowVol"], "s-", label="Yellow", color="#f39c12"
        )
        ax2.plot(df["Timestamp"], df["GreenVol"], "^-", label="Green", color="#27ae60")
        ax2.set_xlabel("Date")
        ax2.set_ylabel("Volume (L)")
        ax2.set_title(f"{display_name} - Volume Trends by Category")
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))

        plt.tight_layout()

        # Save plot
        plot_file = plot_dir / f"{category}_trends.png"
        plt.savefig(plot_file, dpi=150, bbox_inches="tight")
        plt.close()

        plot_files[category] = plot_file.name

    return plot_files


def create_html_dashboard(
    data: Dict[str, pd.DataFrame],
    plot_files: Dict[str, str],
    plot_dir: Path,
    out_file: Optional[Path] = None,
):
    """Create an HTML dashboard for ChemInventory data."""

    plot_dir = Path(plot_dir)

    # Generate timestamp
    generated_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    html_lines = [
        "<!doctype html>",
        '<html lang="en">',
        "<head>",
        '  <meta charset="utf-8" />',
        "  <title>ChemInventory Dashboard</title>",
        "  <style>",
        "    body { font-family: Arial, Helvetica, sans-serif; margin: 20px; background: #f5f5f5; }",
        "    .container { max-width: 1400px; margin: 0 auto; }",
        "    .header { background: white; border-radius: 10px; padding: 30px; margin-bottom: 30px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }",
        "    .header h1 { margin: 0 0 10px 0; color: #2c3e50; }",
        "    .header p { margin: 0; color: #7f8c8d; }",
        "    .category-section { background: white; border-radius: 10px; padding: 25px; margin-bottom: 25px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }",
        "    .category-section h2 { margin: 0 0 20px 0; color: #34495e; border-bottom: 2px solid #3498db; padding-bottom: 10px; }",
        "    .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-bottom: 20px; }",
        "    .stat-card { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 8px; }",
        "    .stat-card.red { background: linear-gradient(135deg, #e74c3c 0%, #c0392b 100%); }",
        "    .stat-card.yellow { background: linear-gradient(135deg, #f39c12 0%, #e67e22 100%); }",
        "    .stat-card.green { background: linear-gradient(135deg, #27ae60 0%, #229954 100%); }",
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
        "      <h1>🧪 Chemical Inventory Environmental Dashboard</h1>",
        f"      <p>Laboratory chemical environmental impact tracking • Generated {generated_time}</p>",
        "    </div>",
    ]

    # Process each category
    for category in CATEGORIES:
        if category not in data or data[category].empty:
            continue

        df = data[category]
        display_name = CATEGORY_DISPLAY_NAMES.get(category, category)

        # Get latest values
        latest = df.iloc[0]
        red_vol = latest["RedVol"]
        yellow_vol = latest["YellowVol"]
        green_vol = latest["GreenVol"]
        total_vol = red_vol + yellow_vol + green_vol
        latest_time = latest["Timestamp"]

        html_lines += [
            '    <div class="category-section">',
            f"      <h2>{display_name}</h2>",
            '      <div class="summary">',
            f"        <h3>Latest Reading ({latest_time})</h3>",
            f"        <p><strong>Total Volume:</strong> {total_vol:.2f} L</p>",
            "      </div>",
            '      <div class="stats-grid">',
            '        <div class="stat-card red">',
            "          <h3>Red Zone</h3>",
            f'          <div class="value">{red_vol:.1f}</div>',
            '          <div class="unit">Litres</div>',
            "        </div>",
            '        <div class="stat-card yellow">',
            "          <h3>Yellow Zone</h3>",
            f'          <div class="value">{yellow_vol:.1f}</div>',
            '          <div class="unit">Litres</div>',
            "        </div>",
            '        <div class="stat-card green">',
            "          <h3>Green Zone</h3>",
            f'          <div class="value">{green_vol:.1f}</div>',
            '          <div class="unit">Litres</div>',
            "        </div>",
            "      </div>",
        ]

        # Add plot if available
        if category in plot_files:
            html_lines.append(
                f'      <img src="{plot_files[category]}" alt="{display_name} trends" />'
            )

        # Add data table (last 10 records)
        html_lines += [
            "      <h3>Recent History (Last 10 Records)</h3>",
            "      <table>",
            "        <thead>",
            "          <tr>",
            "            <th>Timestamp</th>",
            "            <th>Red (L)</th>",
            "            <th>Yellow (L)</th>",
            "            <th>Green (L)</th>",
            "            <th>Total (L)</th>",
            "          </tr>",
            "        </thead>",
            "        <tbody>",
        ]

        for _, row in df.head(10).iterrows():
            total = row["RedVol"] + row["YellowVol"] + row["GreenVol"]
            html_lines.append(
                f"          <tr><td>{row['Timestamp']}</td><td>{row['RedVol']:.2f}</td><td>{row['YellowVol']:.2f}</td><td>{row['GreenVol']:.2f}</td><td>{total:.2f}</td></tr>"
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
        out_file = plot_dir / "cheminventory_dashboard.html"
    else:
        out_file = Path(out_file)

    with out_file.open("w", encoding="utf-8") as fh:
        fh.write("\n".join(html_lines))

    print(f"Dashboard created: {out_file}")
    return out_file


def main():
    parser = argparse.ArgumentParser(
        description="Generate ChemInventory dashboard from SQL Server"
    )
    parser.add_argument(
        "--plot-dir", default="plots", help="Directory for plots (default: plots)"
    )
    parser.add_argument(
        "--out", help="Output HTML file (default: plots/cheminventory_dashboard.html)"
    )
    parser.add_argument(
        "--connection-string",
        help="Custom SQL connection string (optional)",
    )

    args = parser.parse_args()

    connection_string = args.connection_string or CONNECTION_STRING
    plot_dir = Path(args.plot_dir)

    print("Fetching data from SQL Server...")
    data = fetch_all_data(connection_string)

    if not data:
        print("No data found in database. Please run ChemInventory_sqlserver.py first.")
        return

    print(f"Found data for {len(data)} categories")

    print("Creating plots...")
    plot_files = create_plots(data, plot_dir)

    print("Creating HTML dashboard...")
    out_file = Path(args.out) if args.out else None
    create_html_dashboard(data, plot_files, plot_dir, out_file)

    print("Done!")


if __name__ == "__main__":
    main()
