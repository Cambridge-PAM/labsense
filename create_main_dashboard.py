"""Create a main index dashboard that links to all detailed dashboards.

This generates an HTML landing page that provides links to:
- Waste dashboard
- ChemInventory dashboard
- Orders dashboard
- Other analytics dashboards
"""

import os
from pathlib import Path
import argparse
from datetime import datetime
from typing import List, Dict, Optional

from dotenv import load_dotenv


# Load environment variables from Labsense_SQL/.env
load_dotenv(Path(__file__).resolve().parent / "Labsense_SQL" / ".env")


def scan_dashboards(plot_dir: Path) -> List[Dict[str, str]]:
    """Scan the plots directory for dashboard HTML files."""
    dashboards = []

    # Look for HTML dashboard files
    for html_file in plot_dir.glob("*dashboard.html"):
        name = html_file.stem.replace("_dashboard", "").replace("_", " ").title()
        dashboards.append(
            {
                "name": name,
                "file": html_file.name,
                "path": html_file.name,  # Use filename only for relative links
            }
        )

    return sorted(dashboards, key=lambda x: x["name"])


def create_main_dashboard(plot_dir: Path, out_file: Optional[Path] = None):
    """Create the main index dashboard HTML."""

    plot_dir = Path(plot_dir)
    if os.name == "nt" and plot_dir.drive:
        drive_root = Path(f"{plot_dir.drive}\\")
        if not drive_root.exists():
            raise FileNotFoundError(
                f"Drive '{plot_dir.drive}' is not available in this session. "
                "Map the drive first or use a UNC path (e.g. \\\\server\\share\\LabsenseDashboard)."
            )

    if not plot_dir.exists():
        plot_dir.mkdir(parents=True, exist_ok=True)

    # Determine output file early so we can compute relative asset paths.
    if out_file is None:
        out_file = plot_dir / "index.html"
    else:
        out_file = Path(out_file)

    # Resolve logo path (supports running from repo root or plot directory).
    logo_candidates = [
        plot_dir / "labsense_logo.png",
        Path(__file__).resolve().parent / "labsense_logo.png",
        plot_dir.parent / "labsense_logo.png",
    ]
    logo_path = next(
        (candidate for candidate in logo_candidates if candidate.exists()), None
    )
    if logo_path is not None:
        try:
            logo_src = os.path.relpath(logo_path, start=out_file.parent).replace(
                "\\", "/"
            )
        except ValueError:
            # Different drive letters (for example C: source and Z: output)
            # cannot be represented as a relative path on Windows.
            target_logo_path = out_file.parent / logo_path.name
            if not target_logo_path.exists():
                target_logo_path.write_bytes(logo_path.read_bytes())
            logo_src = logo_path.name
    else:
        logo_src = "labsense_logo.png"

    # Scan for existing dashboards
    dashboards = scan_dashboards(plot_dir)

    # Generate timestamp
    generated_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    html_lines = [
        "<!doctype html>",
        '<html lang="en">',
        "<head>",
        '  <meta charset="utf-8" />',
        "  <title>Labsense Analytics Dashboard</title>",
        "  <style>",
        "    body { font-family: Arial, Helvetica, sans-serif; margin: 0; padding: 0; background: linear-gradient(135deg, #dff5e6 0%, #b7e4c7 100%); min-height: 100vh; }",
        "    .container { max-width: 1100px; margin: 0 auto; padding: 40px 20px; }",
        "    .header { background: white; border-radius: 10px; padding: 30px; margin-bottom: 30px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }",
        "    .header-row { display: flex; align-items: center; gap: 16px; flex-wrap: wrap; }",
        "    .logo { width: 64px; height: 64px; object-fit: contain; }",
        "    .header h1 { margin: 0 0 10px 0; color: #1b4332; }",
        "    .header p { margin: 0; color: #2d6a4f; }",
        "    .dashboard-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 20px; }",
        "    .dashboard-card { background: white; border-radius: 10px; padding: 25px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); transition: transform 0.2s, box-shadow 0.2s; text-decoration: none; display: block; }",
        "    .dashboard-card:hover { transform: translateY(-5px); box-shadow: 0 8px 12px rgba(27,67,50,0.18); }",
        "    .dashboard-card h2 { margin: 0 0 10px 0; color: #2d6a4f; font-size: 1.3em; }",
        "    .dashboard-card p { margin: 0; color: #666; font-size: 0.9em; }",
        "    .dashboard-card .arrow { float: right; color: #40916c; font-size: 1.5em; }",
        "    .footer { text-align: center; color: #1b4332; margin-top: 40px; font-size: 0.9em; }",
        "    .no-dashboards { background: white; border-radius: 10px; padding: 30px; text-align: center; color: #666; }",
        "  </style>",
        "</head>",
        "<body>",
        '  <div class="container">',
        '    <div class="header">',
        '      <div class="header-row">',
        f'        <img src="{logo_src}" alt="Labsense logo" class="logo" />',
        "        <div>",
        "          <h1>Labsense Analytics Dashboard</h1>",
        f"          <p>Laboratory management and monitoring system • Generated {generated_time}</p>",
        "        </div>",
        "      </div>",
        "    </div>",
    ]

    if dashboards:
        html_lines.append('    <div class="dashboard-grid">')

        for dash in dashboards:
            html_lines += [
                f'      <a href="{dash["path"]}" class="dashboard-card">',
                f'        <span class="arrow">→</span>',
                f'        <h2>{dash["name"]}</h2>',
                f'        <p>View detailed {dash["name"].lower()} analytics and reports</p>',
                "      </a>",
            ]

        html_lines.append("    </div>")
    else:
        html_lines += [
            '    <div class="no-dashboards">',
            "      <h2>No Dashboards Found</h2>",
            f"      <p>No *dashboard.html files were found in: {plot_dir}</p>",
            "      <p>Run the individual dashboard generators to create detailed analytics.</p>",
            "    </div>",
        ]

    html_lines += [
        '    <div class="footer">',
        "      <p>Labsense • University of Cambridge</p>",
        "    </div>",
        "  </div>",
        "</body>",
        "</html>",
    ]

    with out_file.open("w", encoding="utf-8") as fh:
        fh.write("\n".join(html_lines))

    print(f"Main dashboard created: {out_file}")
    print(f"Found {len(dashboards)} dashboard(s)")

    return out_file


def main():
    plots_dir_default = os.getenv("PLOTS_DIR", "plots")
    parser = argparse.ArgumentParser(description="Create main Labsense dashboard")
    parser.add_argument(
        "--plot-dir",
        default=plots_dir_default,
        help=f"Directory containing dashboard files (default: {plots_dir_default})",
    )
    parser.add_argument("--out", help="Output HTML file (default: plots/index.html)")

    args = parser.parse_args()

    create_main_dashboard(
        plot_dir=Path(args.plot_dir), out_file=Path(args.out) if args.out else None
    )


if __name__ == "__main__":
    main()
