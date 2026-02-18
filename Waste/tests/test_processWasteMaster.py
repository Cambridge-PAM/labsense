import pytest
import pandas as pd
from Waste import processWasteMaster as pwm


def approx(a, b, tol=1e-6):
    return abs(a - b) <= tol


def test_compute_hp_volume_basic():
    df = pd.DataFrame(
        {
            "Date": ["2026-02-01", "2026-02-01", "2026-02-01"],
            "Size": [2.5, 2.5, 2.5],
            "Unit": ["L", "L", "L"],
            "HP1": [1, 0, 1],
            "HP2": [0, 1, 0],
            "HP3": [1, 1, 1],
        }
    )
    res = pwm.compute_hp_volume(df)
    mapping = {r["HP Number"]: r["Volume(L)"] for _, r in res.iterrows()}

    assert approx(mapping["HP1"], 2.5)
    assert approx(mapping["HP2"], 1.25)
    assert approx(mapping["HP3"], 3.75)


def test_compute_hp_volume_splits_evenly_across_active_hp_flags():
    df = pd.DataFrame(
        {
            "Date": ["2026-02-01"],
            "Size": [2.5],
            "Unit": ["L"],
            "HP1": [1],
            "HP2": [1],
            "HP3": [0],
        }
    )
    res = pwm.compute_hp_volume(df)
    mapping = {r["HP Number"]: r["Volume(L)"] for _, r in res.iterrows()}

    assert approx(mapping["HP1"], 1.25)
    assert approx(mapping["HP2"], 1.25)
    assert approx(mapping["HP3"], 0.0)


def test_compute_hp_volume_units_and_unknown_unit(capfd):
    df = pd.DataFrame(
        {
            "Date": ["2026-02-01", "2026-02-01"],
            "Size": [2500, 1],
            "Unit": ["ml", "unknown_unit"],
            "HP1": [1, 1],
        }
    )
    res = pwm.compute_hp_volume(df)
    mapping = {r["HP Number"]: r["Volume(L)"] for _, r in res.iterrows()}
    assert approx(mapping["HP1"], 3.5)


def test_compute_hp_volume_unnamed_columns_fallback():
    df = pd.DataFrame(
        {
            "Unnamed: 0": ["2026-02-01", "2026-02-02"],
            "Unnamed: 3": [2.5, 1.0],
            "Unnamed: 4": ["L", "L"],
            "HP1": [1, 0],
            "HP2": [0, 1],
        }
    )
    res = pwm.compute_hp_volume(df)
    keymap = {(r["Date"], r["HP Number"]): r["Volume(L)"] for _, r in res.iterrows()}
    assert approx(keymap[(pd.to_datetime("2026-02-01").date(), "HP1")], 2.5)
    assert approx(keymap[(pd.to_datetime("2026-02-02").date(), "HP2")], 1.0)
