"""Generate Electricity Consumption dashboard from SQL Server data.

Queries the labsense SQL Server database for daily and minute-level electricity
consumption data and creates visualizations and an HTML dashboard.
"""

import argparse
import importlib
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

import pyodbc
import pandas as pd

# =============================================================================
# CONFIGURATION TOGGLE
# =============================================================================
# Set to True to calculate idle power from 1am-5am consumption and show active consumption
CALCULATE_IDLE_POWER = True
# =============================================================================

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
    idle_data = df.loc[(df["Hour"] >= 1) & (df["Hour"] < 5)].copy()

    if idle_data.empty:
        print("Warning: No data found for idle hours (1am-5am)")
        return 0.0

    # EnergyValue is energy consumption in kWh for the 1-minute interval
    # Convert to average power: Power (kW) = Energy (kWh) / Time (hours)
    # Time = 1 minute = 1/60 hour, so Power = Energy / (1/60) = Energy * 60
    idle_data.loc[:, "Power_kW"] = idle_data["EnergyValue"] * 60

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

    try:
        savgol_filter = importlib.import_module("scipy.signal").savgol_filter
    except Exception:
        savgol_filter = None

    try:
        dbscan_cls = importlib.import_module("sklearn.cluster").DBSCAN
    except Exception:
        dbscan_cls = None

    if df.empty:
        return {}

    plot_dir = Path(plot_dir)
    plot_dir.mkdir(parents=True, exist_ok=True)

    plot_files = {}

    # Filter to last two years window, anchored to the 1st of the start month
    two_years_ago = datetime.now() - timedelta(days=730)
    last_two_years_start = two_years_ago.replace(
        day=1, hour=0, minute=0, second=0, microsecond=0
    )
    df_last_year = df[df["Datestamp"] >= last_two_years_start].copy()

    if df_last_year.empty:
        print("No data in the last two years")
        return {}

    # Sort by date for plotting
    df_sorted = df_last_year.sort_values("Datestamp")
    df_sorted["Esum_7d_ma"] = df_sorted["Esum"].rolling(window=7, min_periods=1).mean()

    # Create daily consumption trend plot (last year only)
    _fig, ax = plt.subplots(figsize=(12, 6))
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
        "Daily Electricity Consumption Trends (Last Two Years)",
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

    _fig, ax = plt.subplots(figsize=(12, 6))
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
        "Monthly Electricity Consumption (Last Two Years)",
        fontsize=14,
        fontweight="bold",
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

        _fig, ax = plt.subplots(figsize=(14, 6))

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

        today_midnight = datetime.now().replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
        previous_day_start = today_midnight - timedelta(days=1)
        previous_day_end = today_midnight
        previous_day_data = df_gran[
            (df_gran["Timestamp"] >= previous_day_start)
            & (df_gran["Timestamp"] < previous_day_end)
        ].copy()
        if not previous_day_data.empty:
            previous_day_data = previous_day_data.sort_values(by=["Timestamp"]).copy()
            # Rolling 5-minute net change: sum minute-to-minute active-power deltas.
            prev_day_idx = previous_day_data.set_index("Timestamp")
            prev_day_idx["Active_Power_Delta_5min_kW"] = (
                prev_day_idx["Active_Power_kW"].diff().fillna(0.0).rolling("5min").sum()
            )
            previous_day_data["Active_Power_Delta_5min_kW"] = (
                prev_day_idx["Active_Power_Delta_5min_kW"].fillna(0.0).values
            )

            raw_delta = previous_day_data["Active_Power_Delta_5min_kW"].astype(float)
            smoothed_delta = raw_delta.copy()
            smoothed_delta_label = "Smoothed Delta (raw fallback)"
            if savgol_filter is not None and len(raw_delta) >= 7:
                window_length = min(
                    9,
                    len(raw_delta) if len(raw_delta) % 2 == 1 else len(raw_delta) - 1,
                )
                if window_length >= 7:
                    smoothed_delta = pd.Series(
                        savgol_filter(
                            raw_delta.to_numpy(),
                            window_length=window_length,
                            polyorder=2,
                            mode="interp",
                        ),
                        index=raw_delta.index,
                    )
                    smoothed_delta_label = "Smoothed Delta (Savitzky-Golay)"

            residual = raw_delta - smoothed_delta
            median_residual = float(residual.median())
            mad = float((residual - median_residual).abs().median())
            noise_sigma = 1.4826 * mad if mad > 0 else float(residual.std())
            # Lower multiplier increases sensitivity to smaller delta excursions.
            noise_threshold = 2.0 * noise_sigma if noise_sigma > 0 else 0.0

            _fig, (ax_power, ax_delta) = plt.subplots(
                2,
                1,
                figsize=(14, 8),
                sharex=True,
                gridspec_kw={"height_ratios": [3, 1]},
            )

            if CALCULATE_IDLE_POWER and idle_power_kw > 0:
                ax_power.plot(
                    previous_day_data["Timestamp"],
                    previous_day_data["Power_kW"],
                    linewidth=0.8,
                    color="#95a5a6",
                    alpha=0.5,
                    label=f"Total Power (inc. {idle_power_kw:.2f} kW idle)",
                )
                ax_power.plot(
                    previous_day_data["Timestamp"],
                    previous_day_data["Active_Power_kW"],
                    linewidth=1.2,
                    color="#3498db",
                    label="Active Power",
                )
                ax_power.axhline(
                    y=idle_power_kw,
                    color="#e74c3c",
                    linestyle="--",
                    linewidth=1.5,
                    alpha=0.7,
                    label=f"Idle Power ({idle_power_kw:.2f} kW)",
                )
                previous_day_title_suffix = " - Active vs Total"
            else:
                ax_power.plot(
                    previous_day_data["Timestamp"],
                    previous_day_data["Power_kW"],
                    linewidth=1.2,
                    color="#3498db",
                    label="Total Power",
                )
                previous_day_title_suffix = ""

            ax_power.set_ylabel("Power (kW)", fontsize=12)
            ax_power.set_title(
                f"Minute-Level Power Consumption ({previous_day_start.date()}){previous_day_title_suffix}",
                fontsize=14,
                fontweight="bold",
            )
            ax_power.grid(True, alpha=0.3)
            ax_power.legend(loc="upper left")

            ax_delta.plot(
                previous_day_data["Timestamp"],
                raw_delta,
                linewidth=1.0,
                color="#95a5a6",
                alpha=0.45,
                label="Raw Delta (rolling 5-min)",
            )
            ax_delta.plot(
                previous_day_data["Timestamp"],
                smoothed_delta,
                linewidth=1.6,
                color="#8e44ad",
                label=smoothed_delta_label,
            )
            ax_delta.axhline(y=0.0, color="#7f8c8d", linestyle="--", linewidth=1.0)
            if noise_threshold > 0:
                ax_delta.axhline(
                    y=noise_threshold,
                    color="#e67e22",
                    linestyle=":",
                    linewidth=1.0,
                    alpha=0.9,
                    label="Noise Threshold",
                )
                ax_delta.axhline(
                    y=-noise_threshold,
                    color="#e67e22",
                    linestyle=":",
                    linewidth=1.0,
                    alpha=0.9,
                )
                above_noise = smoothed_delta.abs() >= noise_threshold

                # Collect significant local extrema above the noise threshold.
                smoothed_vals = smoothed_delta.to_numpy()
                above_noise_vals = above_noise.to_numpy()
                extrema = []  # list of (data_idx, value)
                for idx in range(1, len(smoothed_vals) - 1):
                    if not above_noise_vals[idx]:
                        continue
                    is_local_max = (
                        smoothed_vals[idx] >= smoothed_vals[idx - 1]
                        and smoothed_vals[idx] > smoothed_vals[idx + 1]
                    )
                    is_local_min = (
                        smoothed_vals[idx] <= smoothed_vals[idx - 1]
                        and smoothed_vals[idx] < smoothed_vals[idx + 1]
                    )
                    if is_local_max or is_local_min:
                        extrema.append((idx, float(smoothed_vals[idx])))

                # Cluster extrema with DBSCAN using |delta| only (no time binning).
                # This ties +/− events of similar magnitude to the same equipment.
                dbscan_eps = noise_threshold * 0.33
                cluster_assignments = {}  # extrema list index -> global cluster id
                cluster_stats = {}  # global cluster id -> {mean_abs, mean_hour}
                next_cluster_id = 0

                if dbscan_cls is not None and dbscan_eps > 0 and extrema:
                    abs_values = [[abs(val)] for _, val in extrema]
                    dbscan_labels = dbscan_cls(
                        eps=dbscan_eps,
                        min_samples=3,
                    ).fit_predict(abs_values)

                    for label in sorted(set(dbscan_labels)):
                        member_indices = [
                            i
                            for i, member_label in enumerate(dbscan_labels)
                            if member_label == label
                        ]
                        mean_abs = sum(
                            abs(extrema[m][1]) for m in member_indices
                        ) / len(member_indices)
                        mean_hour = sum(
                            previous_day_data["Timestamp"].iloc[extrema[m][0]].hour
                            + previous_day_data["Timestamp"].iloc[extrema[m][0]].minute
                            / 60.0
                            + previous_day_data["Timestamp"].iloc[extrema[m][0]].second
                            / 3600.0
                            for m in member_indices
                        ) / len(member_indices)
                        cluster_stats[next_cluster_id] = {
                            "mean_abs": mean_abs,
                            "mean_hour": mean_hour,
                        }
                        for m in member_indices:
                            cluster_assignments[m] = next_cluster_id
                        next_cluster_id += 1
                else:
                    # Fallback when DBSCAN isn't available: keep each extrema as its own cluster.
                    for i, (idx, val) in enumerate(extrema):
                        ts = previous_day_data["Timestamp"].iloc[idx]
                        cluster_assignments[i] = next_cluster_id
                        cluster_stats[next_cluster_id] = {
                            "mean_abs": abs(float(val)),
                            "mean_hour": ts.hour
                            + ts.minute / 60.0
                            + ts.second / 3600.0,
                        }
                        next_cluster_id += 1

                # Labels are ranked by absolute magnitude, regardless of event time.
                cluster_label_map = {}
                ordered_cluster_ids = sorted(
                    cluster_stats,
                    key=lambda cid: -cluster_stats[cid]["mean_abs"],
                )
                for rank, cid in enumerate(ordered_cluster_ids):
                    cluster_label_map[cid] = chr(65 + rank)

                cluster_colors = [
                    "#e74c3c",
                    "#2ecc71",
                    "#3498db",
                    "#f39c12",
                    "#9b59b6",
                    "#1abc9c",
                    "#e67e22",
                    "#34495e",
                ]

                for i, (idx, peak_val) in enumerate(extrema):
                    c_id = cluster_assignments[i]
                    c_label = cluster_label_map[c_id]
                    color = cluster_colors[c_id % len(cluster_colors)]
                    ax_delta.annotate(
                        f"{c_label}: {peak_val:.1f}",
                        xy=(previous_day_data["Timestamp"].iloc[idx], peak_val),
                        xytext=(0, 6 if peak_val >= 0 else -6),
                        textcoords="offset points",
                        ha="center",
                        va="bottom" if peak_val >= 0 else "top",
                        fontsize=8,
                        color=color,
                        bbox={
                            "boxstyle": "round,pad=0.15",
                            "fc": "white",
                            "alpha": 0.75,
                            "ec": color,
                        },
                    )

                # Show DBSCAN grouping in hour-bin vs |delta| space.
                if extrema and cluster_stats:
                    fig_cluster, ax_cluster = plt.subplots(figsize=(10, 4.5))

                    for i, (idx, peak_val) in enumerate(extrema):
                        c_id = cluster_assignments[i]
                        color = cluster_colors[c_id % len(cluster_colors)]
                        ts = previous_day_data["Timestamp"].iloc[idx]
                        hour_of_day = ts.hour + ts.minute / 60.0 + ts.second / 3600.0
                        ax_cluster.scatter(
                            hour_of_day,
                            abs(peak_val),
                            color=color,
                            alpha=0.85,
                            s=45,
                            marker="^" if peak_val >= 0 else "v",
                            edgecolors="white",
                            linewidths=0.5,
                        )

                    # Plot cluster centroids and labels.
                    for c_id, stats in cluster_stats.items():
                        color = cluster_colors[c_id % len(cluster_colors)]
                        c_label = cluster_label_map[c_id]
                        ax_cluster.scatter(
                            stats["mean_hour"],
                            stats["mean_abs"],
                            color=color,
                            marker="D",
                            s=90,
                            edgecolors="black",
                            linewidths=0.8,
                            zorder=3,
                        )
                        ax_cluster.annotate(
                            c_label,
                            xy=(stats["mean_hour"], stats["mean_abs"]),
                            xytext=(0, 7),
                            textcoords="offset points",
                            ha="center",
                            va="bottom",
                            fontsize=8,
                            color=color,
                            bbox={
                                "boxstyle": "round,pad=0.12",
                                "fc": "white",
                                "alpha": 0.8,
                                "ec": color,
                            },
                        )

                    ax_cluster.set_title(
                        (
                            "DBSCAN Clusters: Hour-of-Day vs |Delta| "
                            f"(eps={dbscan_eps:.3f} kW)"
                        ),
                        fontsize=12,
                        fontweight="bold",
                    )
                    ax_cluster.set_xlabel("Hour of Day")
                    ax_cluster.set_ylabel("|Delta| (kW over last 5 min)")
                    ax_cluster.set_xlim(-0.5, 23.5)
                    ax_cluster.set_xticks(range(0, 24, 2))
                    ax_cluster.grid(True, alpha=0.3)

                    cluster_plot = (
                        plot_dir
                        / "electricity_consumption_previous_day_cluster_hour_delta.png"
                    )
                    fig_cluster.tight_layout()
                    fig_cluster.savefig(cluster_plot, dpi=150, bbox_inches="tight")
                    plt.close(fig_cluster)
                    plot_files["previous_day_cluster_hour_delta"] = cluster_plot.name
                    print(f"Created plot: {cluster_plot}")
            ax_delta.set_ylabel("Delta (kW over last 5 min)", fontsize=11)
            ax_delta.set_xlabel("Time", fontsize=12)
            ax_delta.grid(True, alpha=0.3)
            ax_delta.legend(loc="upper left")
            ax_delta.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
            plt.xticks(rotation=45, ha="right")

            plt.tight_layout()
            previous_day_plot = plot_dir / "electricity_consumption_previous_day.png"
            plt.savefig(previous_day_plot, dpi=150, bbox_inches="tight")
            plt.close()
            plot_files["previous_day"] = previous_day_plot.name
            plot_files["previous_day_date"] = str(previous_day_start.date())
            print(f"Created plot: {previous_day_plot}")

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
        requested_previous_day = (datetime.now() - timedelta(days=1)).date()
        previous_day_date = requested_previous_day.strftime("%Y-%m-%d")
        previous_day_rows = df[df["Datestamp"].dt.date == requested_previous_day]
        if not previous_day_rows.empty:
            previous_day_consumption = float(previous_day_rows.iloc[0]["Esum"])
        else:
            previous_day_consumption = 0.0

        prior_day_rows = df[
            df["Datestamp"].dt.date == (requested_previous_day - timedelta(days=1))
        ]
        if not prior_day_rows.empty and not previous_day_rows.empty:
            prior_day_consumption = float(prior_day_rows.iloc[0]["Esum"])
            previous_day_change = previous_day_consumption - prior_day_consumption
        else:
            previous_day_change = 0.0

        fun_context = kwh_in_context_three(avg_consumption)

        # Linear trend fit over last year of daily data: % change from fitted start to end.
        # Negative means decrease, positive means increase.
        trend_change_pct = 0.0
        trend_subtext = "Insufficient data"
        one_year_ago = datetime.now() - timedelta(days=365)
        df_trend = df[df["Datestamp"] >= one_year_ago].sort_values(  # type: ignore[call-overload]
            by=["Datestamp"]
        )
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
                else:
                    trend_change_pct = 0.0

                start_date = df_trend.iloc[0]["Datestamp"].strftime("%Y-%m-%d")
                end_date = df_trend.iloc[-1]["Datestamp"].strftime("%Y-%m-%d")
                trend_subtext = f"from {start_date} to {end_date}"

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
        previous_day_consumption = 0.0
        previous_day_date = "N/A"
        previous_day_change = 0.0
        trend_change_pct = 0.0
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
            f'        <div class="value">{trend_change_pct:+.1f}%</div>',
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

        if "previous_day" in plot_files:
            previous_day_plot_date = plot_files.get(
                "previous_day_date", previous_day_date
            )
            html_lines += [
                '    <div class="section">',
                "      <h2>Day View</h2>",
                '      <div class="summary">',
                f"        <p><strong>Date:</strong> {previous_day_plot_date}</p>",
                f"        <p><strong>Consumption:</strong> {previous_day_consumption:.1f} kWh</p>",
                f"        <p><strong>Change vs previous recorded day:</strong> {previous_day_change:+.1f} kWh</p>",
                "      </div>",
                f'      <img src="{plot_files["previous_day"]}" alt="Previous day power consumption" />',
            ]

            if "previous_day_cluster_hour_delta" in plot_files:
                html_lines += [
                    "      <h3>Hour-of-Day vs Delta Clusters</h3>",
                    "      <p><em>Points are local extrema above the noise threshold. Up/down markers indicate positive/negative deltas; color and label indicate DBSCAN clusters based on delta magnitude only.</em></p>",
                    f'      <img src="{plot_files["previous_day_cluster_hour_delta"]}" alt="Hour of day vs delta DBSCAN clusters" />',
                ]

            html_lines += [
                "    </div>",
            ]

        # Granular consumption section (minute-level data for last week) - SHOWN FIRST
        if "granular" in plot_files:
            html_lines += [
                '    <div class="section">',
                "      <h2>Week View</h2>",
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
                "      <h3>Energy Equivalents (Average Daily)</h3>",
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

        # Combined daily and monthly trends section
        html_lines += [
            '    <div class="section">',
            "      <h2>Month View</h2>",
            "      <h3>Daily Consumption Trends</h3>",
        ]

        if "daily" in plot_files:
            html_lines.append(
                f'      <img src="{plot_files["daily"]}" alt="Daily electricity consumption trends" />'
            )
        else:
            html_lines.append("      <p><em>Daily trend plot not available</em></p>")

        html_lines += [
            "      <h3>Monthly Consumption</h3>",
        ]

        if "monthly" in plot_files:
            html_lines.append(
                f'      <img src="{plot_files["monthly"]}" alt="Monthly electricity consumption" />'
            )
        else:
            html_lines.append(
                "      <p><em>Monthly consumption plot not available</em></p>"
            )

        if not monthly_avg.empty:
            html_lines += [
                "      <h3>Average Daily Consumption</h3>",
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
            ]

        html_lines += [
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
