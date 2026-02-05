import types
from Labsense_SQL import ChemInventory_sqlserver as cis


class FakeResponse:
    def __init__(self, data):
        self._data = data

    def json(self):
        return self._data

    def raise_for_status(self):
        return None


def test_main_dry_run_summaries(monkeypatch):
    # Limit the chemicals processed
    monkeypatch.setattr(cis, 'gsk_2016', {'MyChem': '75-09-2'})
    # Mark this CAS as composite red so it contributes to composite totals
    monkeypatch.setattr(cis, 'gsk_composite_red', {'x': '75-09-2'})

    # Fake API response with one N/A and one valid entry (500 ml)
    containers = [
        {'size': 'N/A', 'unit': 'ml', 'location': 1},
        {'size': '500', 'unit': 'ml', 'location': 1},
    ]
    fake = FakeResponse({'data': {'containers': containers}})

    def fake_post(self, url, json=None, timeout=None):
        return fake

    monkeypatch.setattr('requests.sessions.Session.post', fake_post)

    summary = cis.main(dry_run=True)

    assert summary['skipped_total'] == 1
    # composite red is first element in the composite list; 500 ml => 0.5 L
    assert abs(summary['composite'][0] - 0.5) < 1e-9


def test_sizes_to_litres_handles_whitespace_and_units():
    sizes = [' 50 ', '100']
    units = [' ml', 'mL']
    total, skipped = cis.sizes_to_litres(sizes, units)
    assert skipped == 0
    assert abs(total - (0.05 + 0.1)) < 1e-9
