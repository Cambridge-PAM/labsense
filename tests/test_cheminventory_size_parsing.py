from Labsense_SQL.ChemInventory_sqlserver import sizes_to_litres


def test_valid_conversion():
    sizes = [1000, 500]
    units = ["ml", "ml"]
    total, skipped = sizes_to_litres(sizes, units)
    assert skipped == 0
    assert abs(total - (1.0 + 0.5)) < 1e-9


def test_invalid_size_skipped():
    sizes = ["N/A", "250"]
    units = ["ml", "ml"]
    total, skipped = sizes_to_litres(sizes, units)
    assert skipped == 1
    assert abs(total - 0.25) < 1e-9


def test_unknown_unit_skipped():
    sizes = [100, 200]
    units = ["weird", "ml"]
    total, skipped = sizes_to_litres(sizes, units)
    assert skipped == 1
    assert abs(total - 0.2) < 1e-9


def test_mixed_types_and_whitespace_units():
    sizes = [" 50 ", 100, None]
    units = [" ml", "mL", "ml"]
    total, skipped = sizes_to_litres(sizes, units)
    # One None size is skipped; units with whitespace should be handled
    assert skipped == 1
    assert abs(total - (0.05 + 0.1)) < 1e-9
