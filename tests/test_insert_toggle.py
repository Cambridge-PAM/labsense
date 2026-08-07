"""Tests for the CHEMINVENTORY_INSERT_TO_SQL environment toggle."""

import os
from Labsense_SQL import ChemInventory_sqlserver as cis
from Labsense_SQL import sql_helpers as sh


def test_maybe_insert_skips_when_disabled(monkeypatch):
    # Ensure env var disables SQL writes.
    os.environ["CHEMINVENTORY_INSERT_TO_SQL"] = "False"
    connect_calls = {"count": 0}

    def fake_connect(_connection_string):
        connect_calls["count"] += 1
        raise AssertionError("Connection should not be attempted when disabled")

    monkeypatch.setattr(sh.pyodbc, "connect", fake_connect)

    cis.maybe_insert("chemComposite", [0, 0, 0, None], connection_string="dummy")
    assert connect_calls["count"] == 0


def test_maybe_insert_calls_when_enabled(monkeypatch):
    os.environ["CHEMINVENTORY_INSERT_TO_SQL"] = "True"
    captured = {
        "connection_string": None,
        "insert_payload": None,
        "committed": False,
        "closed": False,
    }

    class FakeCursor:
        def execute(self, query, *params):
            if "INSERT INTO" in query:
                captured["insert_payload"] = params[0] if params else None

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            captured["committed"] = True

        def close(self):
            captured["closed"] = True

    def fake_connect(connection_string):
        captured["connection_string"] = connection_string
        return FakeConnection()

    monkeypatch.setattr(sh.pyodbc, "connect", fake_connect)

    row = [1, 2, 3, None]
    conn = "DRIVER={x};SERVER=.;DATABASE=;"
    cis.maybe_insert("chemVOC", row, connection_string=conn)

    assert captured["connection_string"] == conn
    assert captured["insert_payload"] == (1, 2, 3, None)
    assert captured["committed"] is True
    assert captured["closed"] is True
