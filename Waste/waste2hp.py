"""
Simple extractor/cleaner for Hazardous waste Excel form.

Usage:
    python waste2hp.py [--excel PATH] [--sheet SHEETNAME] [--out-csv PATH] [--out-json PATH]

Output: cleaned CSV and JSON with normalized columns and parsed hazard lists.
"""

from pathlib import Path
import pandas as pd
import re
import argparse
import json
from datetime import date

DEFAULT_XLSX = Path(__file__).parent / "Hazardous waste form - RCE 2025July25.xlsx"
DEFAULT_SHEET = "1.Waste Form"

# Mapping of substrings in source headers to normalized names
COLUMN_MAP = {
    "reference": "reference",
    "chemical name": "chemical_name",
    "description of waste": "chemical_name",
    "no. of containers": "num_containers",
    "container size": "container_size",
    "physical state": "physical_state",
    "hazard statements": "hazard_statements",
    "hazard properties": "hazard_properties",
}

KEEP_COLUMNS = set(COLUMN_MAP.values())

RE_SPLIT_CODES = re.compile(r"[;,/\+|]+")


def detect_header_row(xls, sheet_name):
    df0 = pd.read_excel(xls, sheet_name=sheet_name, header=None, dtype=str)
    for i, row in df0.iterrows():
        row_text = " ".join([str(x) for x in row.dropna().astype(str)])
        if "reference" in row_text.lower():
            return i
    # fallback to row index 23 (0-based) as observed in this workbook
    return 23


def map_columns(columns):
    mapped = {}
    for col in columns:
        low = str(col).lower()
        target = None
        for k, v in COLUMN_MAP.items():
            if k in low:
                target = v
                break
        if target:
            mapped[col] = target
    return mapped


def parse_codes(cell):
    """Parse a cell containing codes like 'H225; H301+H311+H331; EUH019' into a list
    Splits on semicolons, commas, pluses and other common separators and strips whitespace.
    """
    if pd.isna(cell):
        return []
    s = str(cell)
    parts = RE_SPLIT_CODES.split(s)
    # strip and remove empties
    out = [p.strip() for p in parts if p and p.strip()]
    return out


def normalize_code(s):
    """Normalize a single hazard/property code to uppercase with no spaces and only alphanumerics.
    Also normalize HP codes with leading zeros (e.g., 'HP06') to 'HP6'."""
    if not s:
        return ""
    s2 = re.sub(r"[^A-Za-z0-9]", "", str(s)).upper()
    # Normalize HP codes with leading zeros: HP06 -> HP6
    m = re.match(r"^(HP)(\d+)$", s2)
    if m:
        prefix, num = m.groups()
        try:
            return f"{prefix}{int(num)}"
        except ValueError:
            return s2
    return s2


def normalize_codes_list(lst):
    """Normalize list of codes and remove duplicates preserving order."""
    out = []
    seen = set()
    for item in lst or []:
        norm = normalize_code(item)
        if norm and norm not in seen:
            seen.add(norm)
            out.append(norm)
    return out


def clean_dataframe(df):
    # Drop fully empty cols/rows first
    df = df.dropna(axis=1, how="all")
    df = df.dropna(how="all")

    # Map columns
    col_map = map_columns(df.columns)
    df = df.rename(columns=col_map)

    # Keep only requested columns (if they exist)
    present = [c for c in df.columns if c in KEEP_COLUMNS]
    df = df[present]

    # Normalize column order
    ordered = [
        c
        for c in [
            "reference",
            "chemical_name",
            "num_containers",
            "container_size",
            "physical_state",
            "hazard_statements",
            "hazard_properties",
        ]
        if c in df.columns
    ]
    df = df[ordered]

    # Strip whitespace from string columns
    for c in df.select_dtypes(include=["object"]).columns:
        df[c] = df[c].astype(str).str.strip().replace({"nan": None})

    # Drop rows without a valid reference number
    if "reference" in df.columns:
        # coerce to numeric where possible
        df["reference_numeric"] = pd.to_numeric(df["reference"], errors="coerce")
        df = df.dropna(subset=["reference_numeric"]).drop(columns=["reference_numeric"])
        df["reference"] = df["reference"].astype(str).str.strip()
    else:
        # if no reference column, drop all rows
        df = df.iloc[0:0]

    # Parse hazard statement/property columns into lists
    if "hazard_statements" in df.columns:
        df["hazard_statements"] = df["hazard_statements"].apply(parse_codes)
    if "hazard_properties" in df.columns:
        df["hazard_properties"] = df["hazard_properties"].apply(parse_codes)
        # normalize to consistent style (e.g., 'HP6' not 'HP 6' or 'Hp6')
        df["hazard_properties"] = df["hazard_properties"].apply(normalize_codes_list)
    else:
        # ensure hazard_properties exists as empty lists to make downstream code simpler
        df["hazard_properties"] = [[] for _ in range(len(df))]

    # Expand hazard properties into integer columns HP1..HP15 (1 if present, 0 if not)
    for i in range(1, 16):
        hp_col = f"HP{i}"
        df[hp_col] = (
            df["hazard_properties"]
            .apply(lambda lst, h=hp_col: int(h in lst) if isinstance(lst, list) else 0)
            .astype(int)
        )

    # Normalize numeric container counts if present
    if "num_containers" in df.columns:
        df["num_containers"] = (
            pd.to_numeric(df["num_containers"], errors="coerce").fillna(0).astype(int)
        )

    return df


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--excel", default=str(DEFAULT_XLSX))
    p.add_argument("--sheet", default=DEFAULT_SHEET)
    p.add_argument(
        "--out-csv",
        default=None,
        help="Output CSV path. If not provided, defaults to hazardous_waste_table_cleaned_<date>.csv in the same folder as this script",
    )
    p.add_argument(
        "--out-json",
        default=None,
        help="Output JSON path. If not provided, defaults to hazardous_waste_table_cleaned_<date>.json in the same folder as this script",
    )
    p.add_argument(
        "--date",
        default=date.today().isoformat(),
        help="Date to append to each row (YYYY-MM-DD)",
    )
    args = p.parse_args(argv)

    xlsx = Path(args.excel)
    if not xlsx.exists():
        raise FileNotFoundError(f"Excel file not found: {xlsx}")

    # Check sheet presence and provide fallback to '2.1 Waste Form'
    xlsfile = pd.ExcelFile(xlsx)
    sheet_to_use = args.sheet
    if sheet_to_use not in xlsfile.sheet_names:
        alt = "2.1 Waste Form"
        if alt in xlsfile.sheet_names:
            sheet_to_use = alt
            print(f"Sheet '{args.sheet}' not found - falling back to '{alt}'")
        else:
            # as a last resort try to find any sheet containing 'waste form'
            candidates = [s for s in xlsfile.sheet_names if "waste form" in s.lower()]
            if candidates:
                sheet_to_use = candidates[0]
                print(
                    f"Sheet '{args.sheet}' not found - using '{sheet_to_use}' instead"
                )
            else:
                raise FileNotFoundError(
                    f"Sheet '{args.sheet}' not found. Available sheets: {xlsfile.sheet_names}"
                )

    header_row = detect_header_row(xlsx, sheet_to_use)
    print(f"Detected header row: {header_row} (0-based) - using sheet '{sheet_to_use}'")

    df = pd.read_excel(xlsx, sheet_name=sheet_to_use, header=header_row, dtype=str)
    cleaned = clean_dataframe(df)

    # append date column to each row
    cleaned["date"] = args.date

    # Determine output filenames (include date if not provided)
    base = Path(__file__).parent
    out_csv = (
        Path(args.out_csv)
        if args.out_csv
        else base / f"hazardous_waste_table_cleaned_{args.date}.csv"
    )
    out_json = (
        Path(args.out_json)
        if args.out_json
        else base / f"hazardous_waste_table_cleaned_{args.date}.json"
    )

    cleaned.to_csv(out_csv, index=False)

    # For JSON we want hazard lists to be real JSON lists
    records = cleaned.to_dict(orient="records")
    with out_json.open("w", encoding="utf-8") as fh:
        json.dump(records, fh, ensure_ascii=False, indent=2)

    print(f"Saved cleaned CSV: {out_csv} ({len(cleaned)} rows)")
    print(f"Saved cleaned JSON: {out_json}")


if __name__ == "__main__":
    main()
