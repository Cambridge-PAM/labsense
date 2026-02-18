"""Generate Electricity Consumption dashboard from SQL Server data.

Queries the labsense SQL Server database for daily and minute-level electricity
consumption data and creates visualizations and an HTML dashboard.
"""

# =============================================================================
# CONFIGURATION TOGGLE
# =============================================================================
# Set to True to calculate idle power from 1am-5am consumption and show active consumption
CALCULATE_IDLE_POWER = True
# =============================================================================

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


def fetch_granular_data(connection_string: str, days: int = 7) -> pd.DataFrame:
    """Fetch minute-level electricity consumption data from SQL Server for the last N days.

    Args:
        connection_string: SQL Server connection string
        days: Number of days to fetch (default: 7)

    Returns:
        DataFrame with columns: id, EnergyValue, Timestamp
    """
    try:
        connection = pyodbc.connect(connection_string)
        # Get data for the last N days
        query = f"""
            SELECT id, EnergyValue, Timestamp
            FROM elecMinute
            WHERE Timestamp >= DATEADD(day, -{days}, GETDATE())
            ORDER BY Timestamp ASC
        """
        df = pd.read_sql(query, connection)
        connection.close()

        if not df.empty:
            df["Timestamp"] = pd.to_datetime(df["Timestamp"])

        return df
    except pyodbc.Error as ex:
        print(f"Error fetching granular data: {ex}")
        return pd.DataFrame()


def calculate_idle_power(df_granular: pd.DataFrame) -> float:
    """Calculate idle power consumption from 1am-5am data.

    Args:
        df_granular: DataFrame with minute-level consumption data

    Returns:
        Average power in kW during idle hours (1am-5am), or 0.0 if insufficient data
    """
    if df_granular.empty:
        return 0.0

    df = df_granular.copy()
    df["Hour"] = df["Timestamp"].dt.hour

    # Filter for 1am-5am hours (1, 2, 3, 4)
    idle_data = df[(df["Hour"] >= 1) & (df["Hour"] < 5)]

    if idle_data.empty:
        print("Warning: No data found for idle hours (1am-5am)")
        return 0.0

    # EnergyValue is energy consumption in kWh for the 1-minute interval
    # Convert to average power: Power (kW) = Energy (kWh) / Time (hours)
    # Time = 1 minute = 1/60 hour, so Power = Energy / (1/60) = Energy * 60
    idle_data["Power_kW"] = idle_data["EnergyValue"] * 60

    avg_idle_power = idle_data["Power_kW"].mean()

    print(
        f"Calculated idle power: {avg_idle_power:.3f} kW (from {len(idle_data)} data points)"
    )

    return avg_idle_power


def create_plots(
    df: pd.DataFrame,
    plot_dir: Path,
    df_granular: Optional[pd.DataFrame] = None,
    idle_power_kw: float = 0.0,
):
    """Create visualization plots for electricity consumption data.

    Args:
        df: Daily consumption DataFrame
        plot_dir: Directory to save plots
        df_granular: Optional minute-level consumption DataFrame
        idle_power_kw: Idle power in kW (for calculating active consumption)
    """
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

    # Filter to last year window, anchored to the 1st of the start month
    one_year_ago = datetime.now() - timedelta(days=365)
    last_year_start = one_year_ago.replace(
        day=1, hour=0, minute=0, second=0, microsecond=0
    )
    df_last_year = df[df["Datestamp"] >= last_year_start].copy()

    if df_last_year.empty:
        print("No data in the last year")
        return {}

    # Sort by date for plotting
    df_sorted = df_last_year.sort_values("Datestamp")
    df_sorted["Esum_7d_ma"] = df_sorted["Esum"].rolling(window=7, min_periods=1).mean()

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
        label="Daily Consumption",
    )
    ax.plot(
        df_sorted["Datestamp"],
        df_sorted["Esum_7d_ma"],
        linestyle="--",
        linewidth=2.5,
        color="#e67e22",
        label="7-Day Moving Average",
    )
    ax.set_xlabel("Date", fontsize=12)
    ax.set_ylabel("Daily Consumption (kWh)", fontsize=12)
    ax.set_title(
        "Daily Electricity Consumption Trends (Last Year)",
        fontsize=14,
        fontweight="bold",
    )
    ax.grid(True, alpha=0.3)
    ax.legend()

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

    # Create granular consumption plot (last 7 days with minute-level data)
    if df_granular is not None and not df_granular.empty:
        df_gran = df_granular.copy()

        # EnergyValue is energy consumption in kWh for each 1-minute interval
        # Convert to average power: Power (kW) = Energy (kWh) / Time (hours)
        # Time = 1 minute = 1/60 hour, so Power = Energy / (1/60) = Energy * 60
        df_gran["Power_kW"] = df_gran["EnergyValue"] * 60

        # Calculate active power by subtracting idle power
        df_gran["Active_Power_kW"] = df_gran["Power_kW"] - idle_power_kw
        # Ensure non-negative
        df_gran["Active_Power_kW"] = df_gran["Active_Power_kW"].clip(lower=0)

        fig, ax = plt.subplots(figsize=(14, 6))

        if CALCULATE_IDLE_POWER and idle_power_kw > 0:
            # Plot both total and active power
            ax.plot(
                df_gran["Timestamp"],
                df_gran["Power_kW"],
                linewidth=0.8,
                color="#95a5a6",
                alpha=0.5,
                label=f"Total Power (inc. {idle_power_kw:.2f} kW idle)",
            )
            ax.plot(
                df_gran["Timestamp"],
                df_gran["Active_Power_kW"],
                linewidth=1.2,
                color="#3498db",
                label="Active Power",
            )
            # Add idle power line
            ax.axhline(
                y=idle_power_kw,
                color="#e74c3c",
                linestyle="--",
                linewidth=1.5,
                alpha=0.7,
                label=f"Idle Power ({idle_power_kw:.2f} kW)",
            )
            ylabel = "Power (kW)"
            title_suffix = " - Active vs Total"
        else:
            # Plot only total power
            ax.plot(
                df_gran["Timestamp"],
                df_gran["Power_kW"],
                linewidth=1.2,
                color="#3498db",
                label="Total Power",
            )
            ylabel = "Power (kW)"
            title_suffix = ""

        ax.set_xlabel("Time", fontsize=12)
        ax.set_ylabel(ylabel, fontsize=12)
        ax.set_title(
            f"Minute-Level Power Consumption (Last 7 Days){title_suffix}",
            fontsize=14,
            fontweight="bold",
        )
        ax.grid(True, alpha=0.3)
        ax.legend(loc="upper right")

        # Format x-axis dates
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d %H:%M"))
        plt.xticks(rotation=45, ha="right")

        plt.tight_layout()
        granular_plot = plot_dir / "electricity_consumption_granular.png"
        plt.savefig(granular_plot, dpi=150, bbox_inches="tight")
        plt.close()
        plot_files["granular"] = granular_plot.name
        print(f"Created plot: {granular_plot}")

        # Add idle power info to plot_files for use in dashboard
        if CALCULATE_IDLE_POWER:
            plot_files["idle_power_kw"] = idle_power_kw

            # Calculate idle energy percentage
            total_energy_kwh = df_gran["EnergyValue"].sum()
            if total_energy_kwh > 0:
                # Idle energy over 7 days: idle_power_kw (kW) * 7 days * 24 hours = kWh
                idle_energy_kwh = idle_power_kw * 7 * 24
                idle_percentage = (idle_energy_kwh / total_energy_kwh) * 100
                plot_files["idle_percentage"] = idle_percentage
                print(f"Idle energy percentage: {idle_percentage:.1f}%")

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
        df = df.copy()
        df["Datestamp"] = pd.to_datetime(df["Datestamp"])
        total_consumption = df["Esum"].sum()
        avg_consumption = df["Esum"].mean()
        max_consumption = df["Esum"].max()
        max_row = df.loc[df["Esum"].idxmax()]
        max_date = pd.to_datetime(max_row["Datestamp"]).strftime("%Y-%m-%d")
        total_days = len(df)

        fun_context = kwh_in_context_three(avg_consumption)

        # Linear trend fit over daily data: % decrease from fitted start to end.
        trend_decrease_pct = 0.0
        trend_subtext = "Insufficient data"
        df_trend = df.sort_values("Datestamp")
        if len(df_trend) >= 2:
            x_days = (
                df_trend["Datestamp"] - df_trend["Datestamp"].min()
            ).dt.total_seconds() / 86400.0
            y_kwh = pd.Series(
                pd.to_numeric(df_trend["Esum"], errors="coerce"), index=df_trend.index
            ).fillna(0.0)

            n = float(len(x_days))
            sum_x = float(x_days.sum())
            sum_y = float(y_kwh.sum())
            sum_xx = float((x_days * x_days).sum())
            sum_xy = float((x_days * y_kwh).sum())
            denom = n * sum_xx - (sum_x * sum_x)

            if denom != 0:
                slope = (n * sum_xy - sum_x * sum_y) / denom
                intercept = (sum_y - slope * sum_x) / n

                start_x = float(x_days.iloc[0])
                end_x = float(x_days.iloc[-1])
                start_fit = intercept + slope * start_x
                end_fit = intercept + slope * end_x

                if start_fit > 0:
                    trend_change_pct = ((end_fit - start_fit) / start_fit) * 100
                    trend_decrease_pct = -trend_change_pct
                else:
                    trend_decrease_pct = 0.0

                start_date = df_trend.iloc[0]["Datestamp"].strftime("%Y-%m-%d")
                end_date = df_trend.iloc[-1]["Datestamp"].strftime("%Y-%m-%d")
                trend_subtext = f"Linear trend from {start_date} to {end_date}"

        # Calculate monthly averages
        df_monthly = df.copy()
        df_monthly["YearMonth"] = df_monthly["Datestamp"].dt.to_period("M")
        monthly_avg = df_monthly.groupby("YearMonth")["Esum"].mean().reset_index()
        monthly_avg["YearMonth"] = monthly_avg["YearMonth"].astype(str)
    else:
        total_consumption = avg_consumption = max_consumption = 0
        max_date = "N/A"
        total_days = 0
        monthly_avg = pd.DataFrame()
        fun_context = {"kettles_boiled": 0, "ev_miles": 0, "blue_whale_lifts": 0}
        trend_decrease_pct = 0.0
        trend_subtext = "No data"

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
        "    .stat-card.orange { background: linear-gradient(135deg, #e67e22 0%, #d35400 100%); }",
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
            "        <h3>Consumption Trend</h3>",
            f'        <div class="value">{trend_decrease_pct:.1f}%</div>',
            '        <div class="unit">linear-fit change</div>',
            f'        <div class="subtext">{trend_subtext}</div>',
            "      </div>",
        ]

        # Add idle percentage card if available
        if "idle_percentage" in plot_files:
            html_lines += [
                '      <div class="stat-card orange">',
                "        <h3>Idle Power Percentage</h3>",
                f'        <div class="value">{plot_files["idle_percentage"]:.1f}%</div>',
                '        <div class="unit">of 7-day consumption</div>',
                "      </div>",
            ]

        html_lines += [
            "    </div>",
        ]

        # Granular consumption section (minute-level data for last week) - SHOWN FIRST
        if "granular" in plot_files:
            html_lines += [
                '    <div class="section">',
                "      <h2>Minute-Level Power Consumption (Last 7 Days)</h2>",
            ]

            if "idle_power_kw" in plot_files:
                idle_kw = plot_files["idle_power_kw"]
                html_lines += [
                    '      <div class="summary">',
                    f"        <p><strong>Idle Power (1am-5am average):</strong> {idle_kw:.2f} kW</p>",
                    "        <p>The graph shows average power (kW) calculated from minute-level energy consumption data (kWh).</p>",
                    "        <p>Total power and active power (with idle power subtracted) help identify when equipment is actively being used vs. baseline consumption.</p>",
                    "      </div>",
                ]

            html_lines.append(
                f'      <img src="{plot_files["granular"]}" alt="Minute-level power consumption" />'
            )

            html_lines += [
                "    </div>",
            ]

        # Fun energy equivalents section - moved here
        html_lines += [
            '    <div class="section">',
            "      <h2>Energy Equivalents (Average Daily)</h2>",
            '      <div class="summary">',
            f"        <p>Based on average daily consumption of <strong>{avg_consumption:.1f} kWh/day</strong></p>",
            "      </div>",
            '      <div class="stats-grid">',
            '        <div class="stat-card teal">',
            "          <h3>Kettles Boiled</h3>",
            f'          <div class="value">{fun_context["kettles_boiled"]:.1f}</div>',
            '          <div class="unit">full kettles per day</div>',
            "        </div>",
            '        <div class="stat-card">',
            "          <h3>EV Miles</h3>",
            f'          <div class="value">{fun_context["ev_miles"]:.1f}</div>',
            '          <div class="unit">miles per day</div>',
            "        </div>",
            '        <div class="stat-card yellow">',
            "          <h3>Blue Whale Lifts</h3>",
            f'          <div class="value">{fun_context["blue_whale_lifts"]:.1f}</div>',
            '          <div class="unit">1 m lifts per day</div>',
            "        </div>",
            "      </div>",
            "    </div>",
        ]

        # Daily Consumption Trends section
        html_lines += [
            '    <div class="section">',
            "      <h2>Daily Consumption Trends (Last Year)</h2>",
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

        # Monthly Consumption section
        html_lines += [
            '    <div class="section">',
            "      <h2>Monthly Consumption (Last Year)</h2>",
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
                "      <h2>Monthly Average Consumption</h2>",
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

        # Recent daily consumption table
        html_lines += [
            '    <div class="section">',
            "      <h2>Recent Daily Data (Last 30 Days)</h2>",
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
    plots_dir_default = os.getenv("PLOTS_DIR", "plots")
    parser = argparse.ArgumentParser(
        description="Generate Electricity Consumption dashboard from SQL Server"
    )
    parser.add_argument(
        "--plot-dir",
        default=plots_dir_default,
        help=f"Directory for plots (default: {plots_dir_default})",
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

    print(f"Found {len(df)} daily records")

    # Fetch granular (minute-level) data for the last 7 days
    print("Fetching granular data (last 7 days)...")
    df_granular = fetch_granular_data(connection_string, days=7)
    print(f"Found {len(df_granular)} minute-level records")

    # Calculate idle power if enabled
    idle_power_kw = 0.0
    if CALCULATE_IDLE_POWER and not df_granular.empty:
        print("Calculating idle power from 1am-5am consumption...")
        idle_power_kw = calculate_idle_power(df_granular)

    print("Creating plots...")
    plot_files = create_plots(df, plot_dir, df_granular, idle_power_kw)

    print("Creating HTML dashboard...")
    out_file = Path(args.out) if args.out else None
    create_html_dashboard(df, plot_files, plot_dir, out_file)

    print("Done!")


if __name__ == "__main__":
    main()
