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


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument('--excel', default=str(DEFAULT_PATH), help='Path to Waste Master Excel')
    p.add_argument('--out', default='NewWasteSheet.xlsx', help='Output Excel filename')
    args = p.parse_args(argv)

    df = load_df(Path(args.excel))
    print('Loaded Waste Master with columns:', list(df.columns))
    out = compute_hp_volume(df)
    out.to_excel(args.out, index=False)
    print(f'Saved summary to {args.out}')


if __name__ == '__main__':
    main()

