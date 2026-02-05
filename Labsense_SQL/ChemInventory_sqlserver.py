import json
import sys
from pathlib import Path

# Add repository root to sys.path to allow absolute imports when running this file directly
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from Labsense_SQL.gsk_enviro_dict_temp import (
    gsk_composite_red,
    gsk_composite_yellow,
    gsk_composite_green,
    gsk_inc_red,
    gsk_inc_yellow,
    gsk_inc_green,
    gsk_voc_red,
    gsk_voc_yellow,
    gsk_voc_green,
    gsk_aqua_red,
    gsk_aqua_yellow,
    gsk_aqua_green,
    gsk_air_red,
    gsk_air_yellow,
    gsk_air_green,
    gsk_health_red,
    gsk_health_yellow,
    gsk_health_green,
)
from Labsense_SQL.constants import to_litre, gsk_2016
from Labsense_SQL.sql_helpers import maybe_insert
import os
from dotenv import load_dotenv
import datetime
import requests as req
import pandas as pd
import logging
from typing import List, Tuple, Dict, Any, Optional

# Connection information
# Your SQL Server instance
sqlServerName = "MSM-FPM-70203\\LABSENSE"
# Your database
databaseName = "labsense"
# Use Windows authentication
trusted_connection = "yes"
# Encryption
encryption_pref = "Optional"
# Connection string information
connection_string = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={sqlServerName};"
    f"DATABASE={databaseName};"
    f"Trusted_Connection={trusted_connection};"
    f"Encrypt={encryption_pref}"
)

# `gsk_2016` and `to_litre` are defined in `Labsense_SQL.constants` and
# imported at the top of this module (see `from .constants import ...`).
composite_red_list = []
composite_yellow_list = []
composite_green_list = []
inc_red_list = []
inc_yellow_list = []
inc_green_list = []
voc_red_list = []
voc_yellow_list = []
voc_green_list = []
aqua_red_list = []
aqua_yellow_list = []
aqua_green_list = []
air_red_list = []
air_yellow_list = []
air_green_list = []
health_red_list = []
health_yellow_list = []
health_green_list = []

load_dotenv()  # for getting the CHEMINVENTORY_CONNECTION_STRING, add the conncetion string in the .env file. If it doesn't exist, jus create one in the folder


def sizes_to_litres(size_list: List[Any], unit_list: List[Any]) -> Tuple[float, int]:
    """Convert parallel lists of sizes and units to total litres.

    Normalizes unit strings (strip, lower) before lookup. Skips entries with
    non-numeric sizes or unknown units.

    Returns: (total_litres, skipped_count)
    """
    converted_volumes: List[float] = []
    skipped = 0
    for s, u in zip(size_list, unit_list):
        try:
            s_f = float(s)
        except (TypeError, ValueError):
            logging.debug("Skipping invalid size: %r", s)
            skipped += 1
            continue
        unit_normalized = str(u).strip()
        # try exact match then case-insensitive match
        factor = to_litre.get(unit_normalized)
        if factor is None:
            factor = to_litre.get(unit_normalized.lower())
        if factor is None:
            logging.debug("Skipping unknown unit: %r", u)
            skipped += 1
            continue
        converted_volumes.append(s_f * factor)
    return sum(converted_volumes), skipped


def main(
    timeout: int = 10,
    max_retries: int = 3,
    connection_string: Optional[str] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Fetch ChemInventory data, normalize volumes and optionally insert to SQL.

    Returns a summary dict containing totals and counts.

    Arguments:
        timeout: request timeout in seconds
        max_retries: number of retry attempts for HTTP requests
        connection_string: optional SQL connection string to pass to `maybe_insert`
        dry_run: if True, skip inserts regardless of env var
    """
    # configure logging
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=getattr(logging, log_level, logging.INFO))
    logger = logging.getLogger(__name__)

    # Ensure required env var is present when not running in dry-run mode
    chem_token = os.getenv("CHEMINVENTORY_CONNECTION_STRING")
    if not chem_token and not dry_run:
        logger.error(
            "Missing CHEMINVENTORY_CONNECTION_STRING environment variable. "
            "Create a .env file with CHEMINVENTORY_CONNECTION_STRING=your_token "
            "and do NOT commit it to version control."
        )
        raise RuntimeError(
            "CHEMINVENTORY_CONNECTION_STRING is required when not running with dry_run=True"
        )

    # Setup requests session with retries
    session = req.Session()
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    retries = Retry(
        total=max_retries, backoff_factor=0.5, status_forcelist=(500, 502, 503, 504)
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))

    # local accumulators
    composite_red_list_local: List[float] = []
    composite_yellow_list_local: List[float] = []
    composite_green_list_local: List[float] = []
    inc_red_list_local: List[float] = []
    inc_yellow_list_local: List[float] = []
    inc_green_list_local: List[float] = []
    voc_red_list_local: List[float] = []
    voc_yellow_list_local: List[float] = []
    voc_green_list_local: List[float] = []
    aqua_red_list_local: List[float] = []
    aqua_yellow_list_local: List[float] = []
    aqua_green_list_local: List[float] = []
    air_red_list_local: List[float] = []
    air_yellow_list_local: List[float] = []
    air_green_list_local: List[float] = []
    health_red_list_local: List[float] = []
    health_yellow_list_local: List[float] = []
    health_green_list_local: List[float] = []

    skipped_total = 0

    # Decide which SQL connection string to use when inserting.
    # Prefer an explicit argument, fall back to module-level `connection_string`.
    sql_conn = connection_string if connection_string else globals().get("connection_string")
    if not sql_conn:
        logger.debug("No SQL connection string available; SQL inserts will be skipped unless explicitly provided.")

    for key, value in gsk_2016.items():
        logger.info("Processing %s with CAS %s...", key, value)
        try:
            ci = session.post(
                "https://app.cheminventory.net/api/search/execute",
                json={
                    "authtoken": os.getenv("CHEMINVENTORY_CONNECTION_STRING"),
                    "inventory": 873,
                    "type": "cas",
                    "contents": value,
                },
                timeout=timeout,
            )
            ci.raise_for_status()
        except Exception as exc:
            logger.exception("Failed request for %s (%s): %s", key, value, exc)
            continue

        ci_json_raw = ci.json()
        if isinstance(ci_json_raw, str):
            ci_json_raw = json.loads(ci_json_raw)

        ci_json_data = pd.json_normalize(
            ci_json_raw.get("data", {}).get("containers", [])
        )
        ci_df = pd.DataFrame(ci_json_data)

        if ci_df.empty:
            temp_sum = 0.0
        else:
            ci_df_real = ci_df.loc[ci_df["location"] != 527895]
            if ci_df_real.empty:
                temp_sum = 0.0
            else:
                size = list(ci_df_real.get("size", []))
                unit = list(ci_df_real.get("unit", []))
                temp_sum, skipped = sizes_to_litres(size, unit)
                skipped_total += skipped
                if skipped:
                    logger.info(
                        "Skipped %d container(s) for %s due to missing/invalid size or unit.",
                        skipped,
                        key,
                    )

        # assign into right lists
        if value in gsk_composite_red.values():
            composite_red_list_local.append(temp_sum)
        if value in gsk_composite_yellow.values():
            composite_yellow_list_local.append(temp_sum)
        if value in gsk_composite_green.values():
            composite_green_list_local.append(temp_sum)
        if value in gsk_inc_red.values():
            inc_red_list_local.append(temp_sum)
        if value in gsk_inc_yellow.values():
            inc_yellow_list_local.append(temp_sum)
        if value in gsk_inc_green.values():
            inc_green_list_local.append(temp_sum)
        if value in gsk_voc_red.values():
            voc_red_list_local.append(temp_sum)
        if value in gsk_voc_yellow.values():
            voc_yellow_list_local.append(temp_sum)
        if value in gsk_voc_green.values():
            voc_green_list_local.append(temp_sum)
        if value in gsk_aqua_red.values():
            aqua_red_list_local.append(temp_sum)
        if value in gsk_aqua_yellow.values():
            aqua_yellow_list_local.append(temp_sum)
        if value in gsk_aqua_green.values():
            aqua_green_list_local.append(temp_sum)
        if value in gsk_air_red.values():
            air_red_list_local.append(temp_sum)
        if value in gsk_air_yellow.values():
            air_yellow_list_local.append(temp_sum)
        if value in gsk_air_green.values():
            air_green_list_local.append(temp_sum)
        if value in gsk_health_red.values():
            health_red_list_local.append(temp_sum)
        if value in gsk_health_yellow.values():
            health_yellow_list_local.append(temp_sum)
        if value in gsk_health_green.values():
            health_green_list_local.append(temp_sum)

    # Summarize
    record_timestamp = datetime.datetime.now()
    summary = {
        "timestamp": record_timestamp,
        "composite": [
            sum(composite_red_list_local),
            sum(composite_yellow_list_local),
            sum(composite_green_list_local),
        ],
        "incineration": [
            sum(inc_red_list_local),
            sum(inc_yellow_list_local),
            sum(inc_green_list_local),
        ],
        "voc": [
            sum(voc_red_list_local),
            sum(voc_yellow_list_local),
            sum(voc_green_list_local),
        ],
        "aquatic": [
            sum(aqua_red_list_local),
            sum(aqua_yellow_list_local),
            sum(aqua_green_list_local),
        ],
        "air": [
            sum(air_red_list_local),
            sum(air_yellow_list_local),
            sum(air_green_list_local),
        ],
        "health": [
            sum(health_red_list_local),
            sum(health_yellow_list_local),
            sum(health_green_list_local),
        ],
        "skipped_total": skipped_total,
    }

    # Insert into SQL (unless dry_run)
    if not dry_run:
        maybe_insert(
            "chemComposite",
            [*summary["composite"], record_timestamp],
            connection_string=sql_conn,
        )
        maybe_insert(
            "chemIncineration",
            [*summary["incineration"], record_timestamp],
            connection_string=sql_conn,
        )
        maybe_insert(
            "chemVOC",
            [*summary["voc"], record_timestamp],
            connection_string=sql_conn,
        )
        maybe_insert(
            "chemAquatic",
            [*summary["aquatic"], record_timestamp],
            connection_string=sql_conn,
        )
        maybe_insert(
            "chemAir",
            [*summary["air"], record_timestamp],
            connection_string=sql_conn,
        )
        maybe_insert(
            "chemHealth",
            [*summary["health"], record_timestamp],
            connection_string=sql_conn,
        )

    return summary


# SQL insert helpers were moved to `Labsense_SQL/sql_helpers.py`.
# Use `from .sql_helpers import maybe_insert` and call with `connection_string` when needed.


if __name__ == "__main__":
    main()
