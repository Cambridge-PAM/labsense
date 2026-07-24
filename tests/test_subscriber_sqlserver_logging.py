"""Tests for MQTT subscriber logging."""

from types import SimpleNamespace

from Labsense_SQL import subscriber_sqlserver as subscriber


def test_on_message_logs_ip_address_without_changing_insert(monkeypatch, caplog):
    msg = SimpleNamespace(
        payload=(
            b'{"labId": 1, "sublabId": 2, "ipAddress": "192.168.10.25", '
            b'"measureTimestamp": "2026-07-24 12:00:00", '
            b'"sensorReadings": {"water": 1250}}'
        ),
        topic="water",
    )

    captured = {}

    def fake_insert_sql_water(lab_id, sublab_id, water, timestamp):
        captured["args"] = (lab_id, sublab_id, water, timestamp)
        return True

    monkeypatch.setattr(subscriber, "insert_sql_water", fake_insert_sql_water)
    caplog.set_level("DEBUG", logger=subscriber.logger.name)

    subscriber.on_message(SimpleNamespace(), None, msg)

    assert captured["args"] == (1, 2, 1.25, "2026-07-24 12:00:00")
    assert "Received message from ipAddress=192.168.10.25 on topic water" in caplog.text
    assert "Water reading from ipAddress=192.168.10.25: 1.250L" in caplog.text