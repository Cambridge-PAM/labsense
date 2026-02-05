import pytest
import pandas as pd
from Waste import waste2hp


def test_normalize_code_hp06():
    assert waste2hp.normalize_code("HP06") == "HP6"


@pytest.mark.parametrize(
    "input_s,expected",
    [
        ("Hp 6", "HP6"),
        (" hp06 ", "HP6"),
        ("euh019", "EUH019"),
    ],
)
def test_normalize_code_variants(input_s, expected):
    assert waste2hp.normalize_code(input_s) == expected


def test_parse_codes_splits_and_strips():
    res = waste2hp.parse_codes("H225; H301+H311,EUH019 /H302")
    assert set(["H225", "H301", "H311", "EUH019", "H302"]).issubset(set(res))


def test_normalize_codes_list_dedup_and_order():
    lst = ["HP06", " HP6 ", "hp6", "HP06", "HP3"]
    out = waste2hp.normalize_codes_list(lst)
    assert out == ["HP6", "HP3"]


def test_clean_dataframe_creates_hp_int_columns():
    df = pd.DataFrame(
        {
            "reference": ["1", "2", "x"],
            "hazard_properties": ["HP06;HP3", "hp4", None],
            "num_containers": [1, 2, 3],
        }
    )
    cleaned = waste2hp.clean_dataframe(df)

    # non-numeric reference row should be dropped
    assert "x" not in cleaned["reference"].values

    # check HP flags for first row (reference '1')
    row1 = cleaned[cleaned["reference"] == "1"].iloc[0]
    assert row1["HP3"] == 1
    assert row1["HP6"] == 1

    # check HP flags for second row (reference '2')
    row2 = cleaned[cleaned["reference"] == "2"].iloc[0]
    assert row2["HP4"] == 1

    # ensure HP columns exist HP1..HP15
    for i in range(1, 16):
        assert f"HP{i}" in cleaned.columns
