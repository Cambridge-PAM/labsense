"""Sensirion SEN66 sensor monitor.

Reads SEN66 measurements and publishes them to MQTT using the same publish
pattern as the other Raspberry Pi sensor scripts in this repository.
"""

from __future__ import annotations

import json
import logging
import os
import socket
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv
import paho.mqtt.publish as publish

from sensirion_driver_adapters.i2c_adapter.i2c_channel import (  # type: ignore[reportMissingImports]
    I2cChannel,
)
from sensirion_i2c_driver import (  # type: ignore[reportMissingImports]
    CrcCalculator,
    I2cConnection,
    LinuxI2cTransceiver,
)
from sensirion_i2c_driver.errors import (  # type: ignore[reportMissingImports]
    I2cChecksumError,
)
from sensirion_i2c_sen66 import Sen66Device  # type: ignore[reportMissingImports]


script_dir = Path(__file__).parent
env_path = script_dir / ".env"

if not env_path.exists():
    print(f"Error: .env file not found at {env_path}")
    sys.exit(1)

load_dotenv(dotenv_path=env_path)

# Configure logging
log_file = script_dir / "sensirion.log"
handlers: list[logging.Handler] = [logging.StreamHandler()]

try:
    handlers.append(logging.FileHandler(log_file))
except PermissionError:
    print(f"Warning: Cannot write to log file {log_file}. Logging to console only.")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=handlers,
)
logger = logging.getLogger(__name__)

# MQTT Configuration
MQTT_SERVER = os.getenv("MQTT_SERVER", "").strip()
MQTT_PATH = os.getenv("MQTT_PATH_SEN66", "labsense/sensors/sen66").strip()
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TIMEOUT = int(os.getenv("MQTT_TIMEOUT", "10"))

# Lab Configuration
LAB_ID = int(os.getenv("LAB_ID", "1"))
SUBLAB_ID = int(os.getenv("SUBLAB_ID", "3"))

# Measurement Configuration
MEASUREMENT_INTERVAL = int(os.getenv("MEASUREMENT_INTERVAL", "5"))
PUBLISH_RETRY_COUNT = int(os.getenv("MQTT_RETRY_COUNT", "3"))

if not MQTT_SERVER:
    logger.error("MQTT_SERVER not configured in .env file")
    sys.exit(1)

logger.info("Configuration loaded successfully")
logger.info("MQTT Server: %s:%s, Path: %s", MQTT_SERVER, MQTT_PORT, MQTT_PATH)
logger.info("Lab ID: %s, Sublab ID: %s", LAB_ID, SUBLAB_ID)

shutdown_flag = False


def signal_handler(signum: int, _frame: Any) -> None:
    """Handle shutdown signals gracefully."""
    global shutdown_flag
    logger.info("Received signal %s. Shutting down gracefully...", signum)
    shutdown_flag = True


def _value_or_none(value: Any) -> Optional[float]:
    """Convert sensor values to floats when possible."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def get_pi_ip_address() -> str:
    """Get the Pi IP address for inclusion in MQTT payloads."""
    try:
        result = subprocess.run(
            ["ip", "addr", "show", "wlan0"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        if result.returncode == 0:
            for line in result.stdout.splitlines():
                if "inet " in line and "inet6" not in line:
                    parts = line.strip().split()
                    if len(parts) >= 2:
                        return parts[1].split("/")[0]
    except (FileNotFoundError, subprocess.SubprocessError) as error:
        logger.warning("Unable to read wlan0 IP address: %s", error)

    try:
        result = subprocess.run(
            ["ifconfig", "wlan0"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        if result.returncode == 0:
            for line in result.stdout.splitlines():
                stripped = line.strip()
                if stripped.startswith("inet "):
                    parts = stripped.split()
                    if len(parts) >= 2:
                        return parts[1]
    except (FileNotFoundError, subprocess.SubprocessError) as error:
        logger.warning("Unable to read wlan0 IP address via ifconfig: %s", error)

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect((MQTT_SERVER, MQTT_PORT))
            routed_ip = sock.getsockname()[0]
            if not routed_ip.startswith("127."):
                return routed_ip
    except OSError as error:
        logger.warning("Unable to determine routed IP address: %s", error)

    try:
        hostname_ip = socket.gethostbyname(socket.gethostname())
        if not hostname_ip.startswith("127."):
            return hostname_ip
    except socket.gaierror as error:
        logger.warning("Unable to resolve hostname IP address: %s", error)

    return "unknown"


def read_sensor_values(sensor: Any) -> dict[str, Optional[float]]:
    """Read a single measurement from the SEN66."""
    (
        mass_concentration_pm1p0,
        mass_concentration_pm2p5,
        mass_concentration_pm4p0,
        mass_concentration_pm10p0,
        ambient_humidity,
        ambient_temperature,
        voc_index,
        nox_index,
        co2,
    ) = sensor.read_measured_values_as_integers()

    return {
        "temperature": _value_or_none(ambient_temperature / 200.0),
        "humidity": _value_or_none(ambient_humidity / 100.0),
        "co2": _value_or_none(co2),
        "pm1": _value_or_none(mass_concentration_pm1p0 / 10.0),
        "pm25": _value_or_none(mass_concentration_pm2p5 / 10.0),
        "pm4": _value_or_none(mass_concentration_pm4p0 / 10.0),
        "pm10": _value_or_none(mass_concentration_pm10p0 / 10.0),
        "voc": _value_or_none(voc_index / 10.0),
        "nox": _value_or_none(nox_index / 10.0),
    }


def build_payload(measurement: dict[str, Optional[float]]) -> str:
    """Build a JSON payload for MQTT publication."""
    payload = {
        "labId": LAB_ID,
        "sublabId": SUBLAB_ID,
        "ipAddress": get_pi_ip_address(),
        "sensorReadings": {"sen66": measurement},
        "measureTimestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    return json.dumps(payload)


def publish_mqtt(msg_payload: str, retry_count: int = PUBLISH_RETRY_COUNT) -> bool:
    """Publish message to MQTT with retry logic."""
    for attempt in range(1, retry_count + 1):
        try:
            publish.single(
                MQTT_PATH,
                msg_payload,
                hostname=MQTT_SERVER,
                port=MQTT_PORT,
                keepalive=MQTT_TIMEOUT,
            )
            logger.info("MQTT message published successfully")
            logger.debug("Payload: %s", msg_payload)
            return True
        except ConnectionRefusedError:
            logger.error("MQTT connection refused: check if broker is running")
        except TimeoutError:
            logger.error(
                "MQTT timeout (attempt %s/%s): Broker not responding at %s:%s",
                attempt,
                retry_count,
                MQTT_SERVER,
                MQTT_PORT,
            )
        except (OSError, RuntimeError, ValueError, TypeError) as error:
            logger.error(
                "MQTT publish error (attempt %s/%s): %s",
                attempt,
                retry_count,
                error,
            )

        if attempt < retry_count:
            time.sleep(2)

    return False


def run() -> None:
    """Entry point for the SEN66 publisher."""
    global shutdown_flag

    logger.info("Starting Sensirion SEN66 monitoring script")
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        with LinuxI2cTransceiver("/dev/i2c-1") as transceiver:
            channel = I2cChannel(
                I2cConnection(transceiver),
                slave_address=0x6B,
                crc=CrcCalculator(8, 0x31, 0xFF, 0x0),
            )
            sensor = Sen66Device(channel)
            try:
                sensor.stop_measurement()
                time.sleep(0.05)
            except Exception:
                pass
            sensor.start_continuous_measurement()
            time.sleep(1.1)
            logger.info("SEN66 continuous measurement started")

            while not shutdown_flag:
                try:
                    _, data_ready = sensor.get_data_ready()
                    if not data_ready:
                        time.sleep(MEASUREMENT_INTERVAL)
                        continue

                    measurement = read_sensor_values(sensor)
                    payload = build_payload(measurement)

                    logger.info(
                        "Readings at %s: temp=%s C, humidity=%s %%RH, co2=%s ppm, pm2.5=%s ug/m3",
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        measurement["temperature"],
                        measurement["humidity"],
                        measurement["co2"],
                        measurement["pm25"],
                    )

                    if not publish_mqtt(payload):
                        logger.warning("Failed to publish MQTT message after retries")

                    time.sleep(MEASUREMENT_INTERVAL)

                except KeyboardInterrupt:
                    logger.info("Keyboard interrupt received")
                    break
                except I2cChecksumError as error:
                    logger.warning("SEN66 checksum error while reading data: %s", error)
                    time.sleep(MEASUREMENT_INTERVAL)
                except (OSError, RuntimeError, ValueError, TypeError) as error:
                    logger.error("Error in measurement loop: %s", error, exc_info=True)
                    time.sleep(MEASUREMENT_INTERVAL)

    except (OSError, RuntimeError, ValueError, TypeError) as error:
        logger.error("Fatal sensor error: %s", error, exc_info=True)
        sys.exit(1)
    finally:
        shutdown_flag = True
        logger.info("Sensirion SEN66 script terminated")


if __name__ == "__main__":
    run()
