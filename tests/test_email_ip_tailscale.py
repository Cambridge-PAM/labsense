import importlib
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = REPO_ROOT / "Labsense_Sensors" / "email_ip.py"


@pytest.fixture
def email_ip_module(tmp_path, monkeypatch):
    env_path = MODULE_PATH.parent / ".env"
    original_content = env_path.read_text() if env_path.exists() else None
    env_path.write_text(
        "EMAIL_USER=test@example.com\n"
        "EMAIL_PASSWORD=app-password\n"
        "EMAIL_SEND=recipient@example.com\n"
    )

    try:
        sys.modules.pop("Labsense_Sensors.email_ip", None)
        monkeypatch.syspath_prepend(str(REPO_ROOT))
        module = importlib.import_module("Labsense_Sensors.email_ip")
        yield module
    finally:
        if original_content is None:
            env_path.unlink(missing_ok=True)
        else:
            env_path.write_text(original_content)
        sys.modules.pop("Labsense_Sensors.email_ip", None)


def test_extract_tailscale_ip_from_network_info(email_ip_module):
    network_info = """Full Network Configuration:
1: lo: <LOOPBACK,UP,LOWER_UP> mtu 65536 qdisc noqueue state UNKNOWN group default qlen 1000
link/loopback 00:00:00:00:00:00 brd 00:00:00:00:00:00
inet 127.0.0.1/8 scope host lo
valid_lft forever preferred_lft forever
2: wlan0: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc pfifo_fast state UP group default qlen 1000
link/ether d8:3a:dd:6b:b1:b2 brd ff:ff:ff:ff:ff:ff
inet 10.247.58.247/18 brd 10.247.63.255 scope global dynamic noprefixroute wlan0
valid_lft 12665sec preferred_lft 11078sec
4: tailscale0: <POINTOPOINT,MULTICAST,NOARP,UP,LOWER_UP> mtu 1280 qdisc pfifo_fast state UNKNOWN group default qlen 500
link/none
inet 100.121.12.54/32 scope global tailscale0
valid_lft forever preferred_lft forever
"""

    assert (
        email_ip_module.get_interface_ip("tailscale0", network_info) == "100.121.12.54"
    )
