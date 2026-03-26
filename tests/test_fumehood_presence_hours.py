import pandas as pd

from Labsense_SQL.Fumehood_dashboard import (
    get_daily_presence_hours,
    get_room_light_presence_data,
)


def test_daily_presence_includes_zero_bar_for_day_with_data_but_no_presence():
    light_df = pd.DataFrame(
        {
            "Timestamp": pd.to_datetime(
                [
                    "2026-01-01 09:00:00",
                    "2026-01-01 10:00:00",
                    "2026-01-02 09:00:00",
                    "2026-01-02 10:00:00",
                ]
            ),
            "Light": [20, 20, 0, 0],
        }
    )

    presence_df = get_room_light_presence_data(light_df, lab_id=1, sublab_id=3)
    assert presence_df is not None

    daily_hours_df = get_daily_presence_hours(presence_df)
    result = {
        row["Day"].strftime("%Y-%m-%d"): row["HoursPresent"]
        for _, row in daily_hours_df.iterrows()
    }

    assert set(result.keys()) == {"2026-01-01", "2026-01-02"}
    assert abs(result["2026-01-01"] - 1.0) < 1e-9
    assert abs(result["2026-01-02"] - 0.0) < 1e-9


def test_weekly_presence_sum_does_not_carry_across_large_no_data_gap():
    light_df = pd.DataFrame(
        {
            "Timestamp": pd.to_datetime(
                [
                    "2026-01-01 09:00:00",
                    "2026-01-01 10:00:00",
                    "2026-01-01 20:00:00",
                    "2026-01-01 21:00:00",
                ]
            ),
            "Light": [20, 20, 0, 0],
        }
    )

    presence_df = get_room_light_presence_data(light_df, lab_id=1, sublab_id=3)
    assert presence_df is not None

    daily_hours_df = get_daily_presence_hours(presence_df)
    weekly_hours_present = daily_hours_df["HoursPresent"].sum()

    assert abs(weekly_hours_present - 1.0) < 1e-9
