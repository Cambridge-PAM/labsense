import network
from utime import sleep
import machine

ssid = "DESKTOP-S2IAEKH 9692"
password = "4@7t588L"


def connect():
    # Connect to WLAN
    wlan = network.WLAN(network.STA_IF)
    wlan.active(True)
    wlan.connect(ssid, password)
    while wlan.isconnected() == False:
        print("Waiting for connection...")
        sleep(1)
    ip = wlan.ifconfig()[0]
    print(f"Connected on {ip}")
    return ip


try:
    ip = connect()
except KeyboardInterrupt:
    machine.reset()
