"""Tests for dashboard timestamp alignment and business-day selection."""

import pandas as pd

from Labsense_SQL.consumption_dashboard import (
    align_presence_to_timestamps,
    get_previous_working_day,
)


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


def test_get_previous_working_day_skips_weekend():
    assert str(get_previous_working_day(pd.Timestamp("2026-04-20 09:00:00"))) == "2026-04-17"
    assert str(get_previous_working_day(pd.Timestamp("2026-04-19 09:00:00"))) == "2026-04-17"
    assert str(get_previous_working_day(pd.Timestamp("2026-04-18 09:00:00"))) == "2026-04-17"
