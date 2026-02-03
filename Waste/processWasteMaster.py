"""Process Waste Master workbook and aggregate volumes per HP code.

Reads the Waste Master Excel (defaults to the path used previously), computes per-date
volume totals for each HP1..HP15 column in litres, and writes a summary workbook.
"""
from pathlib import Path
import argparse
import pandas as pd
import sys

# unit conversion mapping to litres
to_litre = {
    'µl': 1e-6,
    'µL': 1e-6,
    'ul': 1e-6,
    'uL': 1e-6,
    'ml': 1e-3,
    'mL': 1e-3,
    'l': 1.0,
    'L': 1.0,
    'µg': 1.25e-9,
    'ug': 1.25e-9,
    'mg': 1.25e-6,
    'g': 1.25e-3,
    'kg': 1.25,
    'oz': 0.035436875,
    'lb': 0.56699,
    'lbs': 0.56699,
    'gal': 4.54609,
}

DEFAULT_PATH = Path(r"C:\Users\takas\University of Cambridge\MET_PAM - Documents\Lab Management and Safety\Safety\Waste\Waste Master.xlsx")


def load_df(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Waste Master file not found: {path}")
    df = pd.read_excel(path)
    return df


def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Normalize known column names (fallbacks) so code is robust to minor name changes
    colmap = {}
    if 'Date' not in df.columns and 'Unnamed: 0' in df.columns:
        colmap['Unnamed: 0'] = 'Date'
    if 'Size' not in df.columns and 'Unnamed: 3' in df.columns:
        colmap['Unnamed: 3'] = 'Size'
    if 'Unit' not in df.columns and 'Unnamed: 4' in df.columns:
        colmap['Unnamed: 4'] = 'Unit'
    if 'Ref' not in df.columns and 'Unnamed: 1' in df.columns:
        colmap['Unnamed: 1'] = 'Ref'
    if colmap:
        df = df.rename(columns=colmap)
    return df


def compute_hp_volume(df: pd.DataFrame) -> pd.DataFrame:
    # Prepare dataframe
    df = ensure_columns(df)

    if 'Date' not in df.columns:
        raise KeyError("No 'Date' column found in Waste Master")
    # normalize dates to date only
    df['Date'] = pd.to_datetime(df['Date']).dt.date

    # discover HP columns
    hp_cols = [c for c in df.columns if str(c).upper().startswith('HP')]
    if not hp_cols:
        raise KeyError('No HP columns found')

    # Check Size and Unit exist
    if 'Size' not in df.columns or 'Unit' not in df.columns:
        raise KeyError("Expected 'Size' and 'Unit' columns in Waste Master")

    # compute volume in litres per row
    df['unit_key'] = df['Unit'].fillna('').astype(str).str.strip()
    df['unit_mult'] = df['unit_key'].map(lambda x: to_litre.get(x, None))
    missing_units = df['unit_mult'].isna()
    if missing_units.any():
        # warn about missing units and set multiplier to 1.0 for safety
        print('Warning: Some rows have unknown units; setting unit multiplier to 1.0 for those rows', file=sys.stderr)
        df.loc[missing_units, 'unit_mult'] = 1.0

    # ensure Size numeric
    df['Size_numeric'] = pd.to_numeric(df['Size'], errors='coerce').fillna(0.0)
    df['volume_l'] = df['Size_numeric'] * df['unit_mult']

    results = []
    for date, group in df.groupby('Date'):
        for hp in hp_cols:
            # treat hp column as numeric flag or fraction
            vals = pd.to_numeric(group[hp], errors='coerce').fillna(0)
            # sum volume weighted by presence
            total_l = (vals * group['volume_l']).sum()
            results.append({'Date': date, 'HP Number': hp, 'Volume(L)': total_l})
    res_df = pd.DataFrame(results)
    return res_df


def create_summary_plots(res_df: pd.DataFrame, out_prefix: str, plot_dir: Path):
    try:
        import matplotlib.pyplot as plt
    except Exception:
        print('matplotlib not available - skipping plots')
        return

    plot_dir = Path(plot_dir)
    plot_dir.mkdir(parents=True, exist_ok=True)

    # Prepare pivot table for HP volumes
    pivot = res_df.copy()
    pivot['Date'] = pd.to_datetime(pivot['Date'])
    pivot['Quarter'] = pivot['Date'].dt.to_period('Q').astype(str)
    pivot['Year'] = pivot['Date'].dt.year

    # pivot by Quarter and Year
    pivot_q = pivot.pivot_table(index='Quarter', columns='HP Number', values='Volume(L)', aggfunc='sum', fill_value=0)
    pivot_y = pivot.pivot_table(index='Year', columns='HP Number', values='Volume(L)', aggfunc='sum', fill_value=0)

    # ensure HP order HP1..HP15
    hp_cols = [f'HP{i}' for i in range(1, 16)]

    # Stacked quarter plot
    present_hps_q = [c for c in hp_cols if c in pivot_q.columns]
    if present_hps_q:
        pivot_q = pivot_q[present_hps_q]
        fig, ax = plt.subplots(figsize=(12, 6))
        pivot_q.plot(kind='bar', stacked=True, ax=ax, colormap='tab20')
        ax.set_title('Waste volume by quarter per HP code (stacked)')
        ax.set_xlabel('Quarter')
        ax.set_ylabel('Volume (L)')
        plt.xticks(rotation=45, ha='right')
        plt.legend(title='HP Code', bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout()
        fqs = plot_dir / f"{out_prefix}_by_quarter_stacked.png"
        fig.savefig(fqs)
        plt.close(fig)
        print(f'Saved stacked quarter plot: {fqs}')
    else:
        print('No HP columns found for stacked quarter plot')

    # Stacked year plot
    present_hps_y = [c for c in hp_cols if c in pivot_y.columns]
    if present_hps_y:
        pivot_y = pivot_y[present_hps_y]
        fig, ax = plt.subplots(figsize=(10, 6))
        pivot_y.plot(kind='bar', stacked=True, ax=ax, colormap='tab20')
        ax.set_title('Waste volume by year per HP code (stacked)')
        ax.set_xlabel('Year')
        ax.set_ylabel('Volume (L)')
        plt.xticks(rotation=0)
        plt.legend(title='HP Code', bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout()
        fys = plot_dir / f"{out_prefix}_by_year_stacked.png"
        fig.savefig(fys)
        plt.close(fig)
        print(f'Saved stacked year plot: {fys}')
    else:
        print('No HP columns found for stacked year plot')


def create_html_dashboard(res_df: pd.DataFrame, out_prefix: str, plot_dir: Path, out_file: str = None):
    """Create a small HTML dashboard that embeds the stacked plots and shows simple tables."""
    plot_dir = Path(plot_dir)
    plot_q = plot_dir / f"{out_prefix}_by_quarter_stacked.png"
    plot_y = plot_dir / f"{out_prefix}_by_year_stacked.png"

    # Prepare summary tables
    df = res_df.copy()
    df['Date'] = pd.to_datetime(df['Date'])
    q_table = df.groupby(df['Date'].dt.to_period('Q').astype(str))['Volume(L)'].sum().reset_index()
    q_table.columns = ['Quarter', 'Volume(L)']
    y_table = df.groupby(df['Date'].dt.year)['Volume(L)'].sum().reset_index()
    y_table.columns = ['Year', 'Volume(L)']

    html_lines = [
        '<!doctype html>',
        '<html lang="en">',
        '<head>',
        f'  <meta charset="utf-8" />',
        f'  <title>Waste Dashboard - {out_prefix}</title>',
        '  <style>body{font-family:Arial,Helvetica,sans-serif;margin:20px;} .container{max-width:1100px} img{max-width:100%;height:auto;border:1px solid #ccc;padding:4px;background:#fff} table{border-collapse:collapse;margin-top:10px} th,td{padding:6px 8px;border:1px solid #ddd;text-align:left} h2{margin-top:30px}</style>',
        '</head>',
        '<body>',
        '<div class="container">',
        f'  <h1>Waste Dashboard — {out_prefix}</h1>',
        '  <p>Generated by <code>processWasteMaster.py</code></p>',
    ]

    # Embed quarter plot if exists
    if plot_q.exists():
        html_lines += [
            '<h2>Waste by Quarter (stacked by HP)</h2>',
            f'  <img src="{plot_q.name}" alt="Quarter stacked plot" />'
        ]
    else:
        html_lines += ['<p><em>Quarter plot not available</em></p>']

    # Add quarter table
    html_lines += ['<h3>Quarter totals</h3>', '<table><thead><tr><th>Quarter</th><th>Volume (L)</th></tr></thead><tbody>']
    for _, row in q_table.iterrows():
        html_lines.append(f"<tr><td>{row['Quarter']}</td><td>{row['Volume(L)']:.3f}</td></tr>")
    html_lines.append('</tbody></table>')

    # Embed year plot
    if plot_y.exists():
        html_lines += [
            '<h2>Waste by Year (stacked by HP)</h2>',
            f'  <img src="{plot_y.name}" alt="Year stacked plot" />'
        ]
    else:
        html_lines += ['<p><em>Year plot not available</em></p>']

    # Add year table
    html_lines += ['<h3>Year totals</h3>', '<table><thead><tr><th>Year</th><th>Volume (L)</th></tr></thead><tbody>']
    for _, row in y_table.iterrows():
        html_lines.append(f"<tr><td>{int(row['Year'])}</td><td>{row['Volume(L)']:.3f}</td></tr>")
    html_lines.append('</tbody></table>')

    html_lines += ['</div>', '</body>', '</html>']

    out_file = Path(out_file) if out_file else (plot_dir / f"{out_prefix}_dashboard.html")
    with out_file.open('w', encoding='utf-8') as fh:
        fh.write('\n'.join(html_lines))
    print(f'Saved HTML dashboard: {out_file}')


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument('--excel', default=str(DEFAULT_PATH), help='Path to Waste Master Excel')
    p.add_argument('--out', default='NewWasteSheet.xlsx', help='Output Excel filename')
    p.add_argument('--plot-dir', default=None, help='Directory to write plots to (defaults to same dir as --out)')
    p.add_argument('--no-plots', action='store_true', help='Disable plot generation')
    p.add_argument('--dashboard', action='store_true', help='Generate an HTML dashboard (written to plot dir)')
    p.add_argument('--dashboard-file', default=None, help='Path to write the HTML dashboard file')
    args = p.parse_args(argv)

    df = load_df(Path(args.excel))
    print('Loaded Waste Master with columns:', list(df.columns))
    res_df = compute_hp_volume(df)
    res_df.to_excel(args.out, index=False)
    print(f'Saved summary to {args.out}')

    if not args.no_plots:
        out_path = Path(args.out)
        plot_dir = Path(args.plot_dir) if args.plot_dir else out_path.parent / 'plots'
        out_prefix = out_path.stem
        create_summary_plots(res_df, out_prefix, plot_dir)
        if args.dashboard:
            create_html_dashboard(res_df, out_prefix, plot_dir, out_file=args.dashboard_file)


if __name__ == '__main__':
    main()

