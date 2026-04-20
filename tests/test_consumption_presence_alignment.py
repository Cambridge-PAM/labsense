"""Tests for aligning room-light presence to electricity timestamps."""

import pandas as pd

from Labsense_SQL.consumption_dashboard import align_presence_to_timestamps


def test_align_presence_uses_backward_asof_within_tolerance():
    target_df = pd.DataFrame(
        {
            "Timestamp": pd.to_datetime(
                [
                    "2026-01-01 10:00:00",
                    "2026-01-01 10:10:00",
                    "2026-01-01 10:40:00",
                ]
            )
        }
    )
    presence_df = pd.DataFrame(
        {
            "Timestamp": pd.to_datetime(
                [
                    "2026-01-01 09:55:00",
                    "2026-01-01 10:20:00",
                ]
            ),
            "Presence": [1, 0],
        }
    )

    aligned = align_presence_to_timestamps(target_df, presence_df)

    assert aligned.tolist() == [1, 1, 0]


def test_align_presence_defaults_to_zero_when_missing():
    target_df = pd.DataFrame(
        {
            "Timestamp": pd.to_datetime(
                [
                    "2026-01-01 10:00:00",
                    "2026-01-01 10:10:00",
                ]
            )
        }
    )

    aligned = align_presence_to_timestamps(target_df, None)

    assert aligned.tolist() == [0, 0]
