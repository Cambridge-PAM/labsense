"""Process Waste Master workbook and aggregate volumes per HP code.

Reads the Waste Master Excel (defaults to the path used previously), computes per-date
volume totals for each HP1..HP15 column in litres, and writes a summary workbook.

Visualization Strategy:
- TOTAL VOLUME charts show the actual unduplicated waste volume received per period
- ALLOCATION BREAKDOWN charts show what percentage of total waste was assigned to each HP code

This approach clarifies that waste volumes are not double-counted. When a single waste entry
is allocated to multiple HP codes (because it exhibits multiple hazardous properties), the
allocation breakdown charts show the proportions, while total charts show the true volume received.
"""

from pathlib import Path
import argparse
import pandas as pd
import sys
import os
from typing import Optional
from dotenv import load_dotenv

# Add repository root to sys.path to allow absolute imports when running this file directly
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Load environment variables from .env file at repo root
env_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(env_path)

# Unit conversion mapping to litres (centralized)
from Labsense_SQL.constants import to_litre

# `to_litre` moved to `Labsense_SQL.constants` to avoid duplication.


DEFAULT_PATH = Path(r"Z:\\LabsenseDashboard\\Waste Master.xlsx")


def load_df(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Waste Master file not found: {path}")
    df = pd.read_excel(path)
    return df


def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Normalize known column names (fallbacks) so code is robust to minor name changes
    colmap = {}
    if "Date" not in df.columns and "Unnamed: 0" in df.columns:
        colmap["Unnamed: 0"] = "Date"
    if "Size" not in df.columns and "Unnamed: 3" in df.columns:
        colmap["Unnamed: 3"] = "Size"
    if "Unit" not in df.columns and "Unnamed: 4" in df.columns:
        colmap["Unnamed: 4"] = "Unit"
    if "Ref" not in df.columns and "Unnamed: 1" in df.columns:
        colmap["Unnamed: 1"] = "Ref"
    if colmap:
        df = df.rename(columns=colmap)
    return df


def compute_hp_volume(df: pd.DataFrame) -> pd.DataFrame:
    # Prepare dataframe
    df = ensure_columns(df)

    if "Date" not in df.columns:
        raise KeyError("No 'Date' column found in Waste Master")
    # normalize dates to date only
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date  # type: ignore

    # discover HP columns
    hp_cols = [c for c in df.columns if str(c).upper().startswith("HP")]
    if not hp_cols:
        raise KeyError("No HP columns found")

    # Check Size and Unit exist
    if "Size" not in df.columns or "Unit" not in df.columns:
        raise KeyError("Expected 'Size' and 'Unit' columns in Waste Master")

    # compute volume in litres per row
    df["unit_key"] = df["Unit"].fillna("").astype(str).str.strip()
    df["unit_mult"] = df["unit_key"].map(lambda x: to_litre.get(x, None))
    missing_units = df["unit_mult"].isna()
    if missing_units.any():
        # warn about missing units and set multiplier to 1.0 for safety
        print(
            "Warning: Some rows have unknown units; setting unit multiplier to 1.0 for those rows",
            file=sys.stderr,
        )
        df.loc[missing_units, "unit_mult"] = 1.0

    # ensure Size numeric
    size_numeric = pd.to_numeric(df["Size"], errors="coerce")
    df["Size_numeric"] = size_numeric.fillna(0.0)  # type: ignore
    df["volume_l"] = df["Size_numeric"] * df["unit_mult"]

    # Allocate each row volume proportionally across all active HP flags to avoid
    # double counting. For example, a 2.5 L row with HP1=1 and HP2=1 contributes
    # 1.25 L to HP1 and 1.25 L to HP2.
    hp_numeric = df[hp_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
    hp_active = hp_numeric.gt(0)
    active_counts = pd.Series(hp_active.sum(axis=1), index=df.index, dtype="float64")

    # Rows with no active HP flags should contribute zero to all HP totals.
    allocation_base = df["volume_l"] / active_counts.replace(0, pd.NA)
    allocated = hp_active.mul(allocation_base, axis=0).fillna(0.0)

    allocated_with_date = allocated.assign(Date=df["Date"])
    res_df = (
        allocated_with_date.melt(
            id_vars="Date", var_name="HP Number", value_name="Volume(L)"
        )
        .groupby(["Date", "HP Number"], as_index=False)["Volume(L)"]
        .sum()
    )
    return res_df


def create_summary_plots(res_df: pd.DataFrame, out_prefix: str, plot_dir: Path):
    try:
        import matplotlib.pyplot as plt
    except Exception:
        print("matplotlib not available - skipping plots")
        return

    import re

    # sanitize prefix: remove leading 'test' and any existing 'dashboard' suffix
    clean_prefix = re.sub(r"(?i)^(test[_-]+)", "", out_prefix)
    clean_prefix = re.sub(r"(?i)[_-]*dashboard$", "", clean_prefix)
    clean_prefix = clean_prefix.strip("_-") or out_prefix

    plot_dir = Path(plot_dir)
    plot_dir.mkdir(parents=True, exist_ok=True)

    # Prepare pivot tables for HP volumes
    pivot = res_df.copy()
    pivot["Date"] = pd.to_datetime(pivot["Date"])
    pivot["Quarter"] = pivot["Date"].dt.to_period("Q").astype(str)  # type: ignore
    pivot["Year"] = pivot["Date"].dt.year  # type: ignore

    # Total waste volume per period (sum across all HP codes for that period)
    total_q = pivot.groupby("Quarter")["Volume(L)"].sum().reset_index()
    total_q.set_index("Quarter", inplace=True)
    total_y = pivot.groupby("Year")["Volume(L)"].sum().reset_index()
    total_y.set_index("Year", inplace=True)

    # Allocated volumes pivot by Quarter and Year
    pivot_q = pivot.pivot_table(
        index="Quarter",
        columns="HP Number",
        values="Volume(L)",
        aggfunc="sum",
        fill_value=0,
    )
    pivot_y = pivot.pivot_table(
        index="Year",
        columns="HP Number",
        values="Volume(L)",
        aggfunc="sum",
        fill_value=0,
    )

    # Proportion (percentage) of allocated volume by HP code
    pivot_q_pct = pivot_q.div(pivot_q.sum(axis=1), axis=0) * 100
    pivot_y_pct = pivot_y.div(pivot_y.sum(axis=1), axis=0) * 100

    # ensure HP order HP1..HP15
    hp_cols = [f"HP{i}" for i in range(1, 16)]

    # 1. Total waste volume by quarter (unduplicated)
    if len(total_q) > 0:
        fig, ax = plt.subplots(figsize=(12, 5))
        total_q["Volume(L)"].plot(
            kind="bar", ax=ax, color="#3498db", edgecolor="black", linewidth=1.5
        )
        ax.set_title("Total Waste Volume by Quarter", fontsize=14, fontweight="bold")
        ax.set_xlabel("Quarter")
        ax.set_ylabel("Volume (L)")
        ax.grid(axis="y", alpha=0.3)
        plt.xticks(rotation=45, ha="right")
        # Add value labels on bars
        for container in ax.containers:
            ax.bar_label(container, fmt="%.1f L", padding=3)
        plt.tight_layout()
        fq_total = plot_dir / f"{clean_prefix}_total_by_quarter.png"
        fig.savefig(fq_total)
        plt.close(fig)
        print(f"Saved total quarterly volume plot: {fq_total}")

    # 2. HP code allocation breakdown by quarter (100% stacked - proportion)
    present_hps_q = [c for c in hp_cols if c in pivot_q_pct.columns]
    if present_hps_q:
        pivot_q_pct_sel = pivot_q_pct[present_hps_q]
        fig, ax = plt.subplots(figsize=(12, 6))
        pivot_q_pct_sel.plot(kind="bar", stacked=True, ax=ax, colormap="tab20")
        ax.set_title(
            "Waste Allocation Breakdown by Quarter (% of total)",
            fontsize=14,
            fontweight="bold",
        )
        ax.set_xlabel("Quarter")
        ax.set_ylabel("Percentage (%)")
        ax.set_ylim(0, 100)
        plt.xticks(rotation=45, ha="right")
        plt.legend(title="HP Code", bbox_to_anchor=(1.05, 1), loc="upper left")
        plt.tight_layout()
        fqs = plot_dir / f"{clean_prefix}_by_quarter_stacked.png"
        fig.savefig(fqs)
        plt.close(fig)
        print(f"Saved HP allocation by quarter plot: {fqs}")
    else:
        print("No HP columns found for quarterly allocation plot")

    # 3. Total waste volume by year (unduplicated)
    if len(total_y) > 0:
        fig, ax = plt.subplots(figsize=(10, 5))
        total_y["Volume(L)"].plot(
            kind="bar", ax=ax, color="#e74c3c", edgecolor="black", linewidth=1.5
        )
        ax.set_title("Total Waste Volume by Year", fontsize=14, fontweight="bold")
        ax.set_xlabel("Year")
        ax.set_ylabel("Volume (L)")
        ax.grid(axis="y", alpha=0.3)
        plt.xticks(rotation=0)
        # Add value labels on bars
        for container in ax.containers:
            ax.bar_label(container, fmt="%.1f L", padding=3)
        plt.tight_layout()
        fy_total = plot_dir / f"{clean_prefix}_total_by_year.png"
        fig.savefig(fy_total)
        plt.close(fig)
        print(f"Saved total annual volume plot: {fy_total}")

    # 4. HP code allocation breakdown by year (100% stacked - proportion)
    present_hps_y = [c for c in hp_cols if c in pivot_y_pct.columns]
    if present_hps_y:
        pivot_y_pct_sel = pivot_y_pct[present_hps_y]
        fig, ax = plt.subplots(figsize=(10, 6))
        pivot_y_pct_sel.plot(kind="bar", stacked=True, ax=ax, colormap="tab20")
        ax.set_title(
            "Waste Allocation Breakdown by Year (% of total)",
            fontsize=14,
            fontweight="bold",
        )
        ax.set_xlabel("Year")
        ax.set_ylabel("Percentage (%)")
        ax.set_ylim(0, 100)
        plt.xticks(rotation=0)
        plt.legend(title="HP Code", bbox_to_anchor=(1.05, 1), loc="upper left")
        plt.tight_layout()
        fys = plot_dir / f"{clean_prefix}_by_year_stacked.png"
        fig.savefig(fys)
        plt.close(fig)
        print(f"Saved HP allocation by year plot: {fys}")
    else:
        print("No HP columns found for annual allocation plot")


def create_html_dashboard(
    res_df: pd.DataFrame,
    out_prefix: str,
    plot_dir: Path,
    out_file: Optional[str] = None,
):
    """Create an HTML dashboard that embeds the stacked plots and shows summary tables.

    Filenames are sanitized to avoid leading 'test' prefixes or duplicated 'dashboard' suffixes.
    """
    import re
    from datetime import datetime

    # sanitize prefix: remove leading 'test' and any existing 'dashboard' suffix
    clean_prefix = re.sub(r"(?i)^(test[_-]+)", "", out_prefix)
    clean_prefix = re.sub(r"(?i)[_-]*dashboard$", "", clean_prefix)
    clean_prefix = clean_prefix.strip("_-") or out_prefix

    plot_dir = Path(plot_dir)

    # Find quarter/year plots with graceful fallbacks (older runs or glob)
    plot_q = plot_dir / f"{clean_prefix}_by_quarter_stacked.png"
    if not plot_q.exists():
        alt = plot_dir / f"{out_prefix}_by_quarter_stacked.png"
        if alt.exists():
            plot_q = alt
        else:
            found = list(plot_dir.glob("*by_quarter_stacked.png"))
            plot_q = found[0] if found else None

    plot_y = plot_dir / f"{clean_prefix}_by_year_stacked.png"
    if not plot_y.exists():
        alt = plot_dir / f"{out_prefix}_by_year_stacked.png"
        if alt.exists():
            plot_y = alt
        else:
            found = list(plot_dir.glob("*by_year_stacked.png"))
            plot_y = found[0] if found else None

    # Prepare summary tables
    df = res_df.copy()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    date_series = df["Date"]
    q_table = (
        df.groupby(date_series.dt.to_period("Q").astype(str))["Volume(L)"]  # type: ignore
        .sum()
        .reset_index()
    )
    q_table.columns = ["Quarter", "Volume(L)"]
    y_table = df.groupby(date_series.dt.year)["Volume(L)"].sum().reset_index()  # type: ignore
    y_table.columns = ["Year", "Volume(L)"]

    # Calculate total volumes and HP code statistics
    total_volume = res_df["Volume(L)"].sum()
    hp_codes = (
        res_df.groupby("HP Number")["Volume(L)"].sum().sort_values(ascending=False)
    )
    most_common_hp = hp_codes.index[0] if len(hp_codes) > 0 else "N/A"
    most_common_volume = hp_codes.iloc[0] if len(hp_codes) > 0 else 0

    # Use the cleaned prefix for title and header so 'test_' is removed
    display_prefix = clean_prefix.replace("_", " ").title()
    generated_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    html_lines = [
        "<!doctype html>",
        '<html lang="en">',
        "<head>",
        '  <meta charset="utf-8" />',
        "  <title>Waste Dashboard</title>",
        "  <style>",
        "    body { font-family: Arial, Helvetica, sans-serif; margin: 20px; background: #f5f5f5; }",
        "    .container { max-width: 1400px; margin: 0 auto; }",
        "    .header { background: white; border-radius: 10px; padding: 30px; margin-bottom: 30px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }",
        "    .header h1 { margin: 0 0 10px 0; color: #2c3e50; }",
        "    .header p { margin: 0; color: #7f8c8d; }",
        "    .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 15px; margin-bottom: 25px; }",
        "    .stat-card { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }",
        "    .stat-card.orange { background: linear-gradient(135deg, #f39c12 0%, #e67e22 100%); }",
        "    .stat-card.teal { background: linear-gradient(135deg, #1abc9c 0%, #16a085 100%); }",
        "    .stat-card h3 { margin: 0 0 5px 0; font-size: 0.9em; opacity: 0.9; }",
        "    .stat-card .value { font-size: 2em; font-weight: bold; }",
        "    .stat-card .unit { font-size: 0.8em; opacity: 0.9; }",
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
        "  </style>",
        "</head>",
        "<body>",
        '  <div class="container">',
        '    <div class="header">',
        "      <h1>♻️ Waste Dashboard</h1>",
        f"      <p>Hazardous waste tracking and analysis by HP code allocation • Generated {generated_time}</p>",
        '      <div class="summary" style="margin-top: 20px;">',
        "        <h3>About This Dashboard</h3>",
        "        <p>This dashboard tracks hazardous waste volumes received and their allocation across EU hazard property codes (HP1-HP15). Each waste entry may be designated with multiple HP codes if it exhibits multiple hazardous properties.</p>",
        "        <p><strong>Key Concept:</strong> The <em>Total Volume</em> charts show the actual waste received (unduplicated). The <em>Allocation Breakdown</em> charts show what percentage of that waste was assigned to each HP code. A single waste entry may appear in multiple HP code allocations.</p>",
        "      </div>",
        "    </div>",
        '    <div class="stats-grid">',
        '      <div class="stat-card">',
        "        <h3>Total Volume</h3>",
        f'        <div class="value">{total_volume:.1f}</div>',
        '        <div class="unit">Litres</div>',
        "      </div>",
        '      <div class="stat-card orange">',
        "        <h3>Most Common HP Code</h3>",
        f'        <div class="value">{most_common_hp}</div>',
        f'        <div class="unit">{most_common_volume:.1f} L</div>',
        "      </div>",
        '      <div class="stat-card teal">',
        "        <h3>Total HP Codes</h3>",
        f'        <div class="value">{len(hp_codes)}</div>',
        '        <div class="unit">Categories</div>',
        "      </div>",
        "    </div>",
    ]

    # HP Codes Reference Table
    hp_reference = {
        "HP1": (
            "Explosive",
            "Waste which is capable by chemical reaction of producing gas at such a temperature and pressure and at such a speed as to cause damage to the surroundings. Pyrotechnic waste, explosive organic peroxide waste and explosive self-reactive waste is included.",
        ),
        "HP2": (
            "Oxidizing",
            "Waste which may, generally by providing oxygen, cause or contribute to the combustion of other materials.",
        ),
        "HP3": (
            "Flammable",
            "Flammable liquid waste (flash point below 60°C); flammable pyrophoric liquid/solid waste; flammable solid waste; flammable gaseous waste; water reactive waste; or flammable aerosols, self-heating waste, organic peroxides and self-reactive waste.",
        ),
        "HP4": (
            "Irritant",
            "Waste which on application can cause skin irritation or damage to the eye.",
        ),
        "HP5": (
            "Harmful",
            "Waste which can cause specific target organ toxicity either from a single or repeated exposure, or which cause acute toxic effects following aspiration.",
        ),
        "HP6": (
            "Toxic",
            "Waste which can cause acute toxic effects following oral or dermal administration, or inhalation exposure.",
        ),
        "HP7": (
            "Carcinogenic",
            "Waste which induces cancer or increases its incidence.",
        ),
        "HP8": ("Corrosive", "Waste which on application, can cause skin corrosion."),
        "HP9": (
            "Infectious",
            "Waste containing viable micro-organisms or their toxins which are known or reliably believed to cause disease in man or other living organisms.",
        ),
        "HP10": (
            "Toxic for reproduction",
            "Waste which has adverse effects on sexual function and fertility in adult males and females, as well as developmental toxicity in the offspring.",
        ),
        "HP11": (
            "Mutagenic",
            "Waste which may cause a mutation, that is a permanent change in the amount or structure of the genetic material in a cell.",
        ),
        "HP12": (
            "Release of an acute toxic gas",
            "Waste which releases acute toxic gases (Acute Tox. 1, 2 or 3) in contact with water or an acid.",
        ),
        "HP13": (
            "Sensitizing",
            "Waste which contains one or more substances known to cause sensitising effects to the skin or the respiratory organs.",
        ),
        "HP14": (
            "Ecotoxic",
            "Waste which presents or may present immediate or delayed risks for one or more sectors of the environment.",
        ),
        "HP15": (
            "Waste capable of exhibiting a hazardous property",
            "Waste capable of exhibiting a hazardous property listed above not directly displayed by the original waste.",
        ),
    }

    html_lines += [
        '    <div class="section">',
        "      <h2>Hazard Property (HP) Codes Reference</h2>",
        '      <div class="summary">',
        "        <p>HP codes classify hazardous properties of waste according to European Waste Framework Directive.</p>",
        "      </div>",
        "      <table>",
        "        <thead>",
        "          <tr>",
        "            <th style='width: 80px;'>Code</th>",
        "            <th style='width: 200px;'>Property</th>",
        "            <th>Description</th>",
        "          </tr>",
        "        </thead>",
        "        <tbody>",
    ]

    for hp_code in sorted(hp_reference.keys(), key=lambda x: int(x[2:])):
        name, description = hp_reference[hp_code]
        html_lines.append(
            f"          <tr><td><strong>{hp_code}</strong></td><td>{name}</td><td>{description}</td></tr>"
        )

    html_lines += [
        "        </tbody>",
        "      </table>",
        "    </div>",
    ]

    # Quarter section
    html_lines += [
        '    <div class="section">',
        "      <h2>Quarterly Analysis</h2>",
        '      <div class="summary">',
        "        <h3>Understanding the Charts</h3>",
        "        <p><strong>Total Volume Chart:</strong> Shows the actual waste volume received each quarter (unduplicated).</p>",
        "        <p><strong>Allocation Breakdown Chart:</strong> Shows what percentage of the total waste was allocated to each HP code. Note: A single waste entry may be attributed to multiple HP codes if it exhibits multiple hazardous properties.</p>",
        "      </div>",
    ]

    # Find and display total quarter volume plot
    plot_q_total = plot_dir / f"{clean_prefix}_total_by_quarter.png"
    if not plot_q_total.exists():
        found = list(plot_dir.glob("*total_by_quarter.png"))
        plot_q_total = found[0] if found else None

    if plot_q_total and Path(plot_q_total).exists():
        html_lines.append("      <h3>Total Waste Volume by Quarter</h3>")
        html_lines.append(
            f'      <img src="{Path(plot_q_total).name}" alt="Total waste volume by quarter" />'
        )
    else:
        html_lines.append("      <p><em>Total volume chart not available</em></p>")

    # Find and display allocation breakdown plot
    plot_q = plot_dir / f"{clean_prefix}_by_quarter_stacked.png"
    if not plot_q.exists():
        alt = plot_dir / f"{out_prefix}_by_quarter_stacked.png"
        if alt.exists():
            plot_q = alt
        else:
            found = list(plot_dir.glob("*by_quarter_stacked.png"))
            plot_q = found[0] if found else None

    if plot_q and Path(plot_q).exists():
        html_lines.append("      <h3>HP Code Allocation Breakdown by Quarter</h3>")
        html_lines.append(
            f'      <img src="{Path(plot_q).name}" alt="HP allocation by quarter" />'
        )
    else:
        html_lines.append(
            "      <p><em>Allocation breakdown chart not available</em></p>"
        )

    html_lines += [
        "      <h3>Quarterly Totals</h3>",
        "      <table>",
        "        <thead>",
        "          <tr><th>Quarter</th><th>Volume (L)</th></tr>",
        "        </thead>",
        "        <tbody>",
    ]
    for _, row in q_table.iterrows():
        html_lines.append(
            f"          <tr><td>{row['Quarter']}</td><td>{row['Volume(L)']:.3f}</td></tr>"
        )
    html_lines += [
        "        </tbody>",
        "      </table>",
        "    </div>",
    ]

    # Year section
    html_lines += [
        '    <div class="section">',
        "      <h2>Annual Analysis</h2>",
    ]

    # Find and display total year volume plot
    plot_y_total = plot_dir / f"{clean_prefix}_total_by_year.png"
    if not plot_y_total.exists():
        found = list(plot_dir.glob("*total_by_year.png"))
        plot_y_total = found[0] if found else None

    if plot_y_total and Path(plot_y_total).exists():
        html_lines.append("      <h3>Total Waste Volume by Year</h3>")
        html_lines.append(
            f'      <img src="{Path(plot_y_total).name}" alt="Total waste volume by year" />'
        )
    else:
        html_lines.append("      <p><em>Total volume chart not available</em></p>")

    # Find and display allocation breakdown plot
    plot_y = plot_dir / f"{clean_prefix}_by_year_stacked.png"
    if not plot_y.exists():
        alt = plot_dir / f"{out_prefix}_by_year_stacked.png"
        if alt.exists():
            plot_y = alt
        else:
            found = list(plot_dir.glob("*by_year_stacked.png"))
            plot_y = found[0] if found else None

    if plot_y and Path(plot_y).exists():
        html_lines.append("      <h3>HP Code Allocation Breakdown by Year</h3>")
        html_lines.append(
            f'      <img src="{Path(plot_y).name}" alt="HP allocation by year" />'
        )
    else:
        html_lines.append(
            "      <p><em>Allocation breakdown chart not available</em></p>"
        )

    html_lines += [
        "      <h3>Annual Totals</h3>",
        "      <table>",
        "        <thead>",
        "          <tr><th>Year</th><th>Volume (L)</th></tr>",
        "        </thead>",
        "        <tbody>",
    ]
    for _, row in y_table.iterrows():
        html_lines.append(
            f"          <tr><td>{int(row['Year'])}</td><td>{row['Volume(L)']:.3f}</td></tr>"
        )
    html_lines += [
        "        </tbody>",
        "      </table>",
        "    </div>",
    ]

    # Detailed data section
    html_lines += [
        '    <div class="section">',
        "      <h2>Detailed Data by HP Code</h2>",
        '      <div class="summary">',
        "        <h3>How Waste is Allocated to HP Codes</h3>",
        "        <p>Each waste entry in the master ledger specifies quantities for one or more HP codes. The table below shows the allocated volumes per HP code per date. Note that a single waste entry may appear across multiple HP columns if it exhibits multiple hazardous properties. The sum of all HP columns for a given date represents the total waste volume for that date (with potential fractional allocations if a single entry was split across codes).</p>",
        "      </div>",
    ]

    # Create pivot table with dates as rows and HP numbers as columns
    pivot_detail = res_df.pivot_table(
        index="Date",
        columns="HP Number",
        values="Volume(L)",
        aggfunc="sum",
        fill_value=0,
    )
    pivot_detail = pivot_detail.sort_index()

    # Build HTML table
    html_lines += [
        "      <table>",
        "        <thead>",
        "          <tr><th>Date</th>",
    ]
    for hp_col in pivot_detail.columns:
        html_lines.append(f"            <th>{hp_col}</th>")
    html_lines += [
        "          </tr>",
        "        </thead>",
        "        <tbody>",
    ]

    for date_idx, row in pivot_detail.iterrows():
        html_lines.append(f"          <tr><td>{date_idx}</td>")
        for val in row:
            html_lines.append(f"            <td>{val:.3f}</td>")
        html_lines.append("          </tr>")

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

    out_path: Path = (
        Path(out_file) if out_file else (plot_dir / f"{clean_prefix}_dashboard.html")
    )
    with out_path.open("w", encoding="utf-8") as fh:
        fh.write("\n".join(html_lines))
    print(f"Saved HTML dashboard: {out_path}")


def main(argv=None):
    plots_dir_default = os.getenv("PLOTS_DIR", "plots")
    p = argparse.ArgumentParser()
    p.add_argument(
        "--excel", default=str(DEFAULT_PATH), help="Path to Waste Master Excel"
    )
    p.add_argument("--out", default="Waste.xlsx", help="Output Excel filename")
    p.add_argument(
        "--plot-dir",
        default=plots_dir_default,
        help=f"Directory to write plots to (default: {plots_dir_default})",
    )
    p.add_argument("--no-plots", action="store_true", help="Disable plot generation")
    p.add_argument(
        "--no-dashboard",
        action="store_true",
        help="Disable HTML dashboard generation",
    )
    p.add_argument(
        "--dashboard-file", default=None, help="Path to write the HTML dashboard file"
    )
    args = p.parse_args(argv)

    df = load_df(Path(args.excel))
    print("Loaded Waste Master with columns:", list(df.columns))
    res_df = compute_hp_volume(df)
    res_df.to_excel(args.out, index=False)
    print(f"Saved summary to {args.out}")

    if not args.no_plots:
        out_path = Path(args.out)
        plot_dir = Path(args.plot_dir)
        out_prefix = out_path.stem
        create_summary_plots(res_df, out_prefix, plot_dir)
        if not args.no_dashboard:
            create_html_dashboard(
                res_df, out_prefix, plot_dir, out_file=args.dashboard_file
            )


if __name__ == "__main__":
    main()
