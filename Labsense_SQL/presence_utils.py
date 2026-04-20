"""Shared utilities for room-light presence signals.

These helpers are used by both fumehood and electricity dashboards to avoid
duplicating threshold and presence derivation logic.
"""

from typing import Dict, Optional, Tuple

import pandas as pd

# Light threshold configuration: {(lab_id, sublab_id): {...}}
# Light level above light_on_threshold indicates the fumehood light is on.
# Light level above room_light_on_threshold indicates room presence.
LIGHT_THRESHOLDS: Dict[Tuple[int, int], Dict[str, float]] = {
    (1, 3): {"light_on_threshold_lux": 80, "room_light_on_threshold_lux": 18},
}


def get_light_threshold(lab_id: int, sublab_id: int, key: str) -> Optional[float]:
    """Fetch a configured light threshold for a fumehood, if present."""
    thresholds = LIGHT_THRESHOLDS.get((lab_id, sublab_id))
    if thresholds is None:
        return None

    value = thresholds.get(key)
    if value is None:
        return None

    return float(value)


def get_room_light_presence_data(
    light_df: pd.DataFrame, lab_id: int, sublab_id: int
) -> Optional[pd.DataFrame]:
    """Prepare room light presence data using the configured room-light threshold.

    Presence is considered on when light exceeds room_light_on_threshold_lux.

    Args:
        light_df: DataFrame with Timestamp and Light columns
        lab_id: Laboratory ID
        sublab_id: Sublaboratory/Fumehood ID

    Returns:
        Sorted DataFrame with a Presence column containing 1 or 0, or None if
        no room-light threshold is configured.
    """
    threshold = get_light_threshold(lab_id, sublab_id, "room_light_on_threshold_lux")
    if threshold is None or light_df.empty:
        return None

    light_sorted = light_df.sort_values("Timestamp").reset_index(drop=True).copy()
    light_sorted["Presence"] = (light_sorted["Light"] > threshold).astype(int)
    return light_sorted
