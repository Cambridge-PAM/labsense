"""Generate Daily Electricity Consumption dashboard from SQL Server data.

Queries the labsense SQL Server database for daily electricity consumption data
and creates visualizations and an HTML dashboard.
"""

import sys
from pathlib import Path

# Add repository root to sys.path to allow absolute imports when running this file directly
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pyodbc
import pandas as pd
import argparse
from datetime import datetime, timedelta
from typing import Optional

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


def kwh_in_context_three(kwh: float) -> dict:
    """Convert kWh into three equivalents.

    - kettles boiled
    - EV miles
    - energy to lift a blue whale by 1 metre
    """
    kettle_kwh = 0.1  # Boiling a full kettle
    ev_miles_per_kwh = 3.0  # Typical EV efficiency
    whale_lift_kwh = 0.41  # Lift a 150,000 kg blue whale by 1 metre

    return {
        "kettles_boiled": kwh / kettle_kwh,
        "ev_miles": kwh * ev_miles_per_kwh,
        "blue_whale_lifts": kwh / whale_lift_kwh,
    }


def fetch_consumption_data(connection_string: str) -> pd.DataFrame:
    """Fetch daily electricity consumption data from SQL Server."""
    try:
        connection = pyodbc.connect(connection_string)
        query = """
            SELECT id, Esum, Datestamp
            FROM elecDaily
            ORDER BY Datestamp DESC
        """
        df = pd.read_sql(query, connection)
        connection.close()

        if not df.empty:
            df["Datestamp"] = pd.to_datetime(df["Datestamp"])

        return df
    except pyodbc.Error as ex:
        print(f"Error fetching data: {ex}")
        return pd.DataFrame()


def create_plots(df: pd.DataFrame, plot_dir: Path):
    """Create visualization plots for electricity consumption data."""
    try:
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
    except ImportError:
        print("matplotlib not available - skipping plots")
        return {}

    if df.empty:
        return {}

    plot_dir = Path(plot_dir)
    plot_dir.mkdir(parents=True, exist_ok=True)

    plot_files = {}

    # Filter to last year only
    one_year_ago = datetime.now() - timedelta(days=365)
    df_last_year = df[df["Datestamp"] >= one_year_ago].copy()

    if df_last_year.empty:
        print("No data in the last year")
        return {}

    # Sort by date for plotting
    df_sorted = df_last_year.sort_values("Datestamp")

    # Create daily consumption trend plot (last year only)
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(
        df_sorted["Datestamp"],
        df_sorted["Esum"],
        marker="o",
        linestyle="-",
        linewidth=2,
        markersize=4,
        color="#3498db",
    )
    ax.set_xlabel("Date", fontsize=12)
    ax.set_ylabel("Daily Consumption (kWh)", fontsize=12)
    ax.set_title(
        "Daily Electricity Consumption Trends (Last Year)",
        fontsize=14,
        fontweight="bold",
    )
    ax.grid(True, alpha=0.3)

    # Format x-axis dates
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    plt.xticks(rotation=45)

    plt.tight_layout()
    daily_plot = plot_dir / "electricity_consumption_trends.png"
    plt.savefig(daily_plot, dpi=150, bbox_inches="tight")
    plt.close()
    plot_files["daily"] = daily_plot.name
    print(f"Created plot: {daily_plot}")

    # Create monthly consumption plot
    df_monthly = df_sorted.copy()
    df_monthly["YearMonth"] = df_monthly["Datestamp"].dt.to_period("M")
    monthly_data = df_monthly.groupby("YearMonth")["Esum"].sum().reset_index()
    monthly_data["YearMonth"] = monthly_data["YearMonth"].astype(str)

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(
        range(len(monthly_data)),
        monthly_data["Esum"],
        color="#3498db",
        alpha=0.8,
        edgecolor="#2c3e50",
        linewidth=1.5,
    )
    ax.set_xlabel("Month", fontsize=12)
    ax.set_ylabel("Monthly Consumption (kWh)", fontsize=12)
    ax.set_title(
        "Monthly Electricity Consumption (Last Year)", fontsize=14, fontweight="bold"
    )
    ax.set_xticks(range(len(monthly_data)))
    ax.set_xticklabels(monthly_data["YearMonth"], rotation=45, ha="right")
    ax.grid(True, alpha=0.3, axis="y")

    plt.tight_layout()
    monthly_plot = plot_dir / "electricity_consumption_monthly.png"
    plt.savefig(monthly_plot, dpi=150, bbox_inches="tight")
    plt.close()
    plot_files["monthly"] = monthly_plot.name
    print(f"Created plot: {monthly_plot}")

    return plot_files


def create_html_dashboard(
    df: pd.DataFrame,
    plot_files: dict,
    plot_dir: Path,
    out_file: Optional[Path] = None,
):
    """Create an HTML dashboard for daily electricity consumption data."""

    plot_dir = Path(plot_dir)

    # Generate timestamp
    generated_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Calculate statistics
    if not df.empty:
        total_consumption = df["Esum"].sum()
        avg_consumption = df["Esum"].mean()
        max_consumption = df["Esum"].max()
        min_consumption = df["Esum"].min()
        max_date = df.loc[df["Esum"].idxmax(), "Datestamp"].strftime("%Y-%m-%d")
        min_date = df.loc[df["Esum"].idxmin(), "Datestamp"].strftime("%Y-%m-%d")
        total_days = len(df)

        fun_context = kwh_in_context_three(total_consumption)

        # Calculate monthly averages
        df_monthly = df.copy()
        df_monthly["YearMonth"] = df_monthly["Datestamp"].dt.to_period("M")
        monthly_avg = df_monthly.groupby("YearMonth")["Esum"].mean().reset_index()
        monthly_avg["YearMonth"] = monthly_avg["YearMonth"].astype(str)
    else:
        total_consumption = avg_consumption = max_consumption = min_consumption = 0
        max_date = min_date = "N/A"
        total_days = 0
        monthly_avg = pd.DataFrame()
        fun_context = {"kettles_boiled": 0, "ev_miles": 0, "blue_whale_lifts": 0}

    html_lines = [
        "<!doctype html>",
        '<html lang="en">',
        "<head>",
        '  <meta charset="utf-8" />',
        "  <title>Electricity Consumption Dashboard</title>",
        "  <style>",
        "    body { font-family: Arial, Helvetica, sans-serif; margin: 20px; background: #f5f5f5; }",
        "    .container { max-width: 1400px; margin: 0 auto; }",
        "    .header { background: white; border-radius: 10px; padding: 30px; margin-bottom: 30px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }",
        "    .header h1 { margin: 0 0 10px 0; color: #2c3e50; }",
        "    .header p { margin: 0; color: #7f8c8d; }",
        "    .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-bottom: 25px; }",
        "    .stat-card { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }",
        "    .stat-card.yellow { background: linear-gradient(135deg, #f39c12 0%, #e67e22 100%); }",
        "    .stat-card.green { background: linear-gradient(135deg, #27ae60 0%, #229954 100%); }",
        "    .stat-card.red { background: linear-gradient(135deg, #e74c3c 0%, #c0392b 100%); }",
        "    .stat-card.teal { background: linear-gradient(135deg, #1abc9c 0%, #16a085 100%); }",
        "    .stat-card h3 { margin: 0 0 5px 0; font-size: 0.9em; opacity: 0.9; }",
        "    .stat-card .value { font-size: 2em; font-weight: bold; }",
        "    .stat-card .unit { font-size: 0.8em; opacity: 0.9; }",
        "    .stat-card .subtext { font-size: 0.75em; opacity: 0.8; margin-top: 5px; }",
        "    .section { background: white; border-radius: 10px; padding: 25px; margin-bottom: 25px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }",
        "    .section h2 { margin: 0 0 20px 0; color: #34495e; border-bottom: 2px solid #3498db; padding-bottom: 10px; }",
        "    .section h3 { margin: 20px 0 10px 0; color: #34495e; }",
        "    img { max-width: 100%; height: auto; border-radius: 8px; margin: 15px 0; }",
        "    table { border-collapse: collapse; width: 100%; margin-top: 15px; }",
        "    th, td { padding: 10px; border: 1px solid #ddd; text-align: left; }",
        "    th { background: #3498db; color: white; font-weight: 600; }",
        "    tr:nth-child(even) { background: #f9f9f9; }",
        "    tr:hover { background: #ecf0f1; }",
        "    .summary { background: #ecf0f1; padding: 15px; border-radius: 8px; margin-bottom: 20px; }",
        "    .summary h3 { margin: 0 0 10px 0; color: #2c3e50; }",
        "    .summary p { margin: 5px 0; color: #34495e; }",
        "    .no-data { text-align: center; color: #7f8c8d; padding: 40px; }",
        "  </style>",
        "</head>",
        "<body>",
        '  <div class="container">',
        '    <div class="header">',
        "      <h1>⚡ Electricity Consumption Dashboard</h1>",
        f"      <p>Daily electricity usage monitoring and analysis • Generated {generated_time}</p>",
        "    </div>",
    ]

    if not df.empty:
        html_lines += [
            '    <div class="stats-grid">',
            '      <div class="stat-card">',
            "        <h3>Total Consumption</h3>",
            f'        <div class="value">{total_consumption:.1f}</div>',
            '        <div class="unit">kWh</div>',
            f'        <div class="subtext">Over {total_days} days</div>',
            "      </div>",
            '      <div class="stat-card yellow">',
            "        <h3>Average Daily</h3>",
            f'        <div class="value">{avg_consumption:.1f}</div>',
            '        <div class="unit">kWh/day</div>',
            "      </div>",
            '      <div class="stat-card red">',
            "        <h3>Peak Consumption</h3>",
            f'        <div class="value">{max_consumption:.1f}</div>',
            '        <div class="unit">kWh</div>',
            f'        <div class="subtext">{max_date}</div>',
            "      </div>",
            '      <div class="stat-card green">',
            "        <h3>Minimum Consumption</h3>",
            f'        <div class="value">{min_consumption:.1f}</div>',
            '        <div class="unit">kWh</div>',
            f'        <div class="subtext">{min_date}</div>',
            "      </div>",
            "    </div>",
            '    <div class="section">',
            "      <h2>Fun Energy Equivalents</h2>",
            '      <div class="stats-grid">',
            '        <div class="stat-card teal">',
            "          <h3>Kettles Boiled</h3>",
            f'          <div class="value">{fun_context["kettles_boiled"]:.1f}</div>',
            '          <div class="unit">full kettles</div>',
            "        </div>",
            '        <div class="stat-card">',
            "          <h3>EV Miles</h3>",
            f'          <div class="value">{fun_context["ev_miles"]:.1f}</div>',
            '          <div class="unit">miles</div>',
            "        </div>",
            '        <div class="stat-card yellow">',
            "          <h3>Blue Whale Lifts</h3>",
            f'          <div class="value">{fun_context["blue_whale_lifts"]:.1f}</div>',
            '          <div class="unit">1 m lifts</div>',
            "        </div>",
            "      </div>",
            "    </div>",
        ]

        # Trend section
        html_lines += [
            '    <div class="section">',
            "      <h2>Consumption Trends</h2>",
        ]

        if "daily" in plot_files:
            html_lines.append(
                f'      <img src="{plot_files["daily"]}" alt="Daily electricity consumption trends" />'
            )
        else:
            html_lines.append("      <p><em>Daily trend plot not available</em></p>")

        html_lines += [
            "    </div>",
        ]

        # Monthly section
        html_lines += [
            '    <div class="section">',
            "      <h2>Monthly Consumption</h2>",
        ]

        if "monthly" in plot_files:
            html_lines.append(
                f'      <img src="{plot_files["monthly"]}" alt="Monthly electricity consumption" />'
            )
        else:
            html_lines.append(
                "      <p><em>Monthly consumption plot not available</em></p>"
            )

        html_lines += [
            "    </div>",
        ]

        # Monthly averages section
        if not monthly_avg.empty:
            html_lines += [
                '    <div class="section">',
                "      <h2>Monthly Averages</h2>",
                "      <table>",
                "        <thead>",
                "          <tr>",
                "            <th>Month</th>",
                "            <th>Average Daily Consumption (kWh)</th>",
                "          </tr>",
                "        </thead>",
                "        <tbody>",
            ]

            for _, row in monthly_avg.iterrows():
                html_lines.append(
                    f"          <tr><td>{row['YearMonth']}</td><td>{row['Esum']:.2f}</td></tr>"
                )

            html_lines += [
                "        </tbody>",
                "      </table>",
                "    </div>",
            ]

        # Recent data table
        html_lines += [
            '    <div class="section">',
            "      <h2>Recent Daily Consumption (Last 30 Days)</h2>",
            "      <table>",
            "        <thead>",
            "          <tr>",
            "            <th>Date</th>",
            "            <th>Consumption (kWh)</th>",
            "          </tr>",
            "        </thead>",
            "        <tbody>",
        ]

        for _, row in df.head(30).iterrows():
            html_lines.append(
                f"          <tr><td>{row['Datestamp'].strftime('%Y-%m-%d')}</td><td>{row['Esum']:.2f}</td></tr>"
            )

        html_lines += [
            "        </tbody>",
            "      </table>",
            "    </div>",
        ]
    else:
        html_lines += [
            '    <div class="no-data">',
            "      <h2>No Data Available</h2>",
            "      <p>Run daily_consumption_sqlserver.py to populate the database with electricity consumption data.</p>",
            "    </div>",
        ]

    html_lines += [
        "  </div>",
        "</body>",
        "</html>",
    ]

    # Write HTML file
    if out_file is None:
        out_file = plot_dir / "electricity_dashboard.html"
    else:
        out_file = Path(out_file)

    with out_file.open("w", encoding="utf-8") as fh:
        fh.write("\n".join(html_lines))

    print(f"Dashboard created: {out_file}")
    return out_file


def main():
    parser = argparse.ArgumentParser(
        description="Generate Electricity Consumption dashboard from SQL Server"
    )
    parser.add_argument(
        "--plot-dir", default="plots", help="Directory for plots (default: plots)"
    )
    parser.add_argument(
        "--out", help="Output HTML file (default: plots/electricity_dashboard.html)"
    )
    parser.add_argument(
        "--connection-string",
        help="Custom SQL connection string (optional)",
    )

    args = parser.parse_args()

    connection_string = args.connection_string or CONNECTION_STRING
    plot_dir = Path(args.plot_dir)

    print("Fetching data from SQL Server...")
    df = fetch_consumption_data(connection_string)

    if df.empty:
        print(
            "No data found in database. Please run daily_consumption_sqlserver.py first."
        )
        print("Creating dashboard anyway to show structure...")

    print(f"Found {len(df)} records")

    print("Creating plots...")
    plot_files = create_plots(df, plot_dir)

    print("Creating HTML dashboard...")
    out_file = Path(args.out) if args.out else None
    create_html_dashboard(df, plot_files, plot_dir, out_file)

    print("Done!")


if __name__ == "__main__":
    main()
