import os
from Labsense_SQL import ChemInventory_sqlserver as cis
from Labsense_SQL import sql_helpers as sh


def test_maybe_insert_skips_when_disabled():
    # ensure env var disables inserts
    os.environ["CHEMINVENTORY_INSERT_TO_SQL"] = "False"

    called = {"count": 0}

    def fake_insert(category, new_row, connection_string=None):
        called["count"] += 1

    sh.insert_to_sql = fake_insert

    # no connection string provided; should not call insert
    cis.maybe_insert("chemComposite", [0, 0, 0, None])
    assert called["count"] == 0


def test_maybe_insert_calls_when_enabled():
    os.environ["CHEMINVENTORY_INSERT_TO_SQL"] = "True"
    called = {"args": None}

    def fake_insert(category, new_row, connection_string=None):
        called["args"] = (category, tuple(new_row))

    sh.insert_to_sql = fake_insert

    row = [1, 2, 3, None]
    # provide a fake connection string so insertion proceeds
    cis.maybe_insert("chemVOC", row, connection_string="DRIVER={x};SERVER=.;DATABASE=;")
    assert called["args"] == ("chemVOC", tuple(row))
