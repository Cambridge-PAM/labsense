"""Fumehood sensor monitor.

Reads distance and ambient light sensors, publishes measurements to MQTT,
attempts sensor recovery on repeated zero-distance faults, and reboots the Pi
only when recovery fails.
"""

import asyncio
import time
import sys
import signal
from datetime import datetime
import paho.mqtt.publish as publish
import os
import subprocess
from pathlib import Path
from dotenv import load_dotenv
import logging
from typing import Any, Optional

try:
    import VL53L1X  # type: ignore[reportMissingImports]  # distance sensor
except ImportError:  # pragma: no cover - depends on Raspberry Pi hardware image
    VL53L1X = None

try:
    from ltr559 import LTR559  # type: ignore[reportMissingImports]  # light sensor
except ImportError:  # pragma: no cover - depends on Raspberry Pi hardware image
    LTR559 = None

# Configure logging
log_file = Path(__file__).parent / "fumehood.log"
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

# Load environment variables from .env file
env_path = Path(__file__).parent / ".env"
if not env_path.exists():
    logger.error(".env file not found at %s", env_path)
    sys.exit(1)

load_dotenv(dotenv_path=env_path)

# MQTT Configuration
MQTT_SERVER = os.getenv("MQTT_SERVER", "").strip()
MQTT_PATH = os.getenv("MQTT_PATH_FUMEHOOD", "fumehood").strip()
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TIMEOUT = int(os.getenv("MQTT_TIMEOUT", "10"))

# Lab Configuration
LAB_ID = int(os.getenv("LAB_ID", "1"))
SUBLAB_ID = int(os.getenv("SUBLAB_ID", "3"))

# Sensor Configuration
TOF_I2C_BUS = int(os.getenv("TOF_I2C_BUS", "1"))
TOF_I2C_ADDRESS = int(os.getenv("TOF_I2C_ADDRESS", "0x29"), 16)
TOF_RANGING_MODE = int(os.getenv("TOF_RANGING_MODE", "1"))  # 1=Short, 2=Medium, 3=Long

# Measurement Configuration
MEASUREMENT_INTERVAL = int(os.getenv("MEASUREMENT_INTERVAL", "30"))
ZERO_DISTANCE_REBOOT_THRESHOLD = int(
    os.getenv("ZERO_DISTANCE_REBOOT_THRESHOLD", "10")
)
ZERO_LIGHT_REINIT_THRESHOLD = int(os.getenv("ZERO_LIGHT_REINIT_THRESHOLD", "5"))

# Sensor Validation Configuration
DISTANCE_MIN_MM = int(os.getenv("DISTANCE_MIN_MM", "0"))  # Minimum valid distance in mm
DISTANCE_MAX_MM = int(
    os.getenv("DISTANCE_MAX_MM", "4000")
)  # Maximum valid distance in mm
LIGHT_MIN_LUX = float(
    os.getenv("LIGHT_MIN_LUX", "0")
)  # Minimum valid light level in lux
LIGHT_MAX_LUX = float(
    os.getenv("LIGHT_MAX_LUX", "200000")
)  # Maximum valid light level in lux

# Validate required configuration
if not MQTT_SERVER:
    logger.error("MQTT_SERVER not configured in .env file")
    sys.exit(1)

logger.info("Configuration loaded successfully")
logger.info("MQTT Server: %s:%s, Path: %s", MQTT_SERVER, MQTT_PORT, MQTT_PATH)
logger.info("Lab ID: %s, Sublab ID: %s", LAB_ID, SUBLAB_ID)

# Global sensor objects
state: dict[str, Any] = {
    "tof": None,
    "ltr559": None,
    "shutdown_flag": False,
}


def initialize_sensors() -> bool:
    """Initialize all sensors with error handling"""
    success = True

    if VL53L1X is None:
        logger.error("VL53L1X module not available; distance sensor disabled")
        state["tof"] = None
        success = False
    if LTR559 is None:
        logger.error("ltr559 module not available; light sensor disabled")
        state["ltr559"] = None
        success = False

    # Initialize distance sensor (VL53L1X)
    if VL53L1X is not None:
        try:
            logger.info(
                "Initializing distance sensor on I2C bus %s, address %s",
                TOF_I2C_BUS,
                hex(TOF_I2C_ADDRESS),
            )
            state["tof"] = VL53L1X.VL53L1X(
                i2c_bus=TOF_I2C_BUS, i2c_address=TOF_I2C_ADDRESS
            )
            state["tof"].open()
            state["tof"].start_ranging(TOF_RANGING_MODE)
            logger.info(
                "Distance sensor initialized (ranging mode: %s)", TOF_RANGING_MODE
            )
        except (OSError, RuntimeError, ValueError, TypeError) as e:
            logger.error("Failed to initialize distance sensor: %s", e)
            state["tof"] = None
            success = False

    # Initialize light sensor (LTR559)
    if LTR559 is not None:
        try:
            logger.info("Initializing light sensor")
            state["ltr559"] = LTR559()
            logger.info("Light sensor initialized")
        except (OSError, RuntimeError, ValueError, TypeError) as e:
            logger.error("Failed to initialize light sensor: %s", e)
            state["ltr559"] = None
            success = False

    return success


def cleanup_sensors():
    """Clean up sensor resources"""
    if state["tof"] is not None:
        try:
            state["tof"].stop_ranging()
            state["tof"].close()
            logger.info("Distance sensor cleaned up")
        except (OSError, RuntimeError, ValueError, TypeError) as e:
            logger.warning("Error cleaning up distance sensor: %s", e)
        finally:
            state["tof"] = None

    # LTR559 doesn't require explicit cleanup
    state["ltr559"] = None
    logger.info("Sensors cleaned up")


def signal_handler(signum, _frame):
    """Handle shutdown signals gracefully"""
    logger.info("Received signal %s. Shutting down gracefully...", signum)
    state["shutdown_flag"] = True
    cleanup_sensors()
    sys.exit(0)


def read_distance_sensor() -> Optional[float]:
    """Read distance from TOF sensor with error handling"""
    tof = state["tof"]
    if tof is None:
        logger.warning("Distance sensor not initialized")
        return None

    try:
        distance = tof.get_distance()
        logger.debug("Distance reading: %s mm", distance)
        return float(distance)
    except (OSError, RuntimeError, ValueError, TypeError) as e:
        logger.error("Error reading distance sensor: %s", e)
        return None


def read_light_sensor() -> Optional[float]:
    """Read light level from LTR559 sensor with error handling"""
    ltr559 = state["ltr559"]
    if ltr559 is None:
        logger.warning("Light sensor not initialized")
        return None

    try:
        ltr559.update_sensor()
        lux = ltr559.get_lux()
        logger.debug("Light reading: %s lux", lux)
        return float(lux)
    except (OSError, RuntimeError, ValueError, TypeError) as e:
        logger.error("Error reading light sensor: %s", e)
        return None


def validate_distance(distance: Optional[float]) -> bool:
    """Validate that distance reading is within acceptable range"""
    if distance is None:
        return False
    if distance < DISTANCE_MIN_MM or distance > DISTANCE_MAX_MM:
        logger.warning("Distance reading is outside valid range")
        return False
    return True


def validate_light(lux: Optional[float]) -> bool:
    """Validate that light level reading is within acceptable range"""
    if lux is None:
        return False
    if lux < LIGHT_MIN_LUX or lux > LIGHT_MAX_LUX:
        logger.warning(
            "Light level reading %s lux is outside valid range (%s-%s lux)",
            lux,
            LIGHT_MIN_LUX,
            LIGHT_MAX_LUX,
        )
        return False
    return True


def publish_mqtt(msg_payload: str, retry_count: int = 3) -> bool:
    """Publish message to MQTT with retry logic"""
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
            logger.error(
                "MQTT connection refused: Check if MQTT broker is running"
            )
        except TimeoutError:
            logger.error(
                "MQTT timeout (attempt %s/%s): Broker not responding at %s:%s",
                attempt,
                retry_count,
                MQTT_SERVER,
                MQTT_PORT,
            )
        except (OSError, RuntimeError, ValueError, TypeError) as e:
            logger.error(
                "MQTT publish error (attempt %s/%s): %s",
                attempt,
                retry_count,
                e,
            )

        if attempt < retry_count:
            time.sleep(2)

    return False


def reboot_pi(reason: str):
    """Reboot Raspberry Pi after critical sensor fault"""
    logger.critical(reason)
    cleanup_sensors()
    try:
        subprocess.run(["sudo", "reboot"], check=False)
    except (OSError, RuntimeError, ValueError, TypeError) as e:
        logger.error("Failed to execute reboot command: %s", e)


def recover_sensors() -> bool:
    """Attempt sensor recovery before rebooting the Pi"""
    logger.warning("Attempting sensor re-initialization before reboot")
    cleanup_sensors()
    time.sleep(2)
    if initialize_sensors():
        logger.info("Sensor re-initialization successful")
        return True
    logger.error("Sensor re-initialization failed")
    return False


async def main():
    """Main monitoring loop with error handling"""
    logger.info("Starting fumehood monitoring main loop")

    iteration = 0
    consecutive_errors = 0
    max_consecutive_errors = 5
    consecutive_zero_distance = 0
    consecutive_zero_light = 0

    while not state["shutdown_flag"]:
        try:
            iteration += 1
            logger.debug("Starting measurement iteration %s", iteration)

            time_send = datetime.now()

            # Read all sensors
            distance = read_distance_sensor()
            lux = read_light_sensor()
            airflow = 0.0  # Placeholder for future airflow sensor

            # Reboot if repeated zero distance readings indicate sensor lockup
            if distance == 0.0:
                consecutive_zero_distance += 1
                logger.warning(
                    "Zero distance reading detected (%s/%s)",
                    consecutive_zero_distance,
                    ZERO_DISTANCE_REBOOT_THRESHOLD,
                )
                if consecutive_zero_distance >= ZERO_DISTANCE_REBOOT_THRESHOLD:
                    if recover_sensors():
                        consecutive_zero_distance = 0
                        consecutive_errors = 0
                        continue
                    reboot_pi(
                        "Repeated 0.0 mm distance readings detected and recovery "
                        "failed; rebooting Pi"
                    )
                    break
            else:
                consecutive_zero_distance = 0

            # Re-initialize sensors on repeated zero light readings
            if lux == 0.0:
                consecutive_zero_light += 1
                logger.warning(
                    "Zero light reading detected (%s/%s)",
                    consecutive_zero_light,
                    ZERO_LIGHT_REINIT_THRESHOLD,
                )
                if consecutive_zero_light >= ZERO_LIGHT_REINIT_THRESHOLD:
                    if recover_sensors():
                        consecutive_zero_light = 0
                        consecutive_errors = 0
                        continue
                    logger.error(
                        "Repeated 0.0 lux readings detected and recovery failed"
                    )
            else:
                consecutive_zero_light = 0

            # Log readings
            logger.info(
                "Readings at %s: distance=%s mm, light=%s lux, airflow=%s",
                time_send.strftime("%Y-%m-%d %H:%M:%S"),
                distance if distance is not None else "N/A",
                lux if lux is not None else "N/A",
                airflow,
            )

            # Validate sensor readings
            distance_valid = validate_distance(distance)
            light_valid = validate_light(lux)

            # Only send if we have at least one valid sensor reading
            if distance_valid or light_valid:
                msg_payload = str(
                    {
                        "labId": LAB_ID,
                        "sublabId": SUBLAB_ID,
                        "sensorReadings": {
                            "fumehood": {
                                "distance": distance if distance_valid else -1.0,
                                "light": lux if light_valid else -1.0,
                                "airflow": airflow,
                            }
                        },
                        "measureTimestamp": time_send.strftime("%Y-%m-%d %H:%M:%S"),
                    }
                )

                if publish_mqtt(msg_payload):
                    consecutive_errors = 0
                else:
                    consecutive_errors += 1
                    logger.warning(
                        "Failed to publish MQTT message (%s/%s)",
                        consecutive_errors,
                        max_consecutive_errors,
                    )
            else:
                logger.warning(
                    "No valid sensor readings available, skipping MQTT publish"
                )
                consecutive_errors += 1

            # Check for too many consecutive errors
            if consecutive_errors >= max_consecutive_errors:
                logger.error(
                    "Too many consecutive failures (%s). "
                    "Check sensor connections and MQTT broker.",
                    consecutive_errors,
                )
                consecutive_errors = 0  # Reset to avoid log spam

            # Wait before next measurement
            await asyncio.sleep(MEASUREMENT_INTERVAL)

        except asyncio.CancelledError:
            logger.info("Main loop cancelled")
            break
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
            break
        except (OSError, RuntimeError, ValueError, TypeError) as e:
            logger.error("Error in main loop: %s", e, exc_info=True)
            consecutive_errors += 1
            await asyncio.sleep(5)  # Backoff on error

    logger.info("Main monitoring loop stopped")


def run():
    """Entry point with full initialization and cleanup"""
    logger.info("=" * 60)
    logger.info("Fumehood Monitoring Script Starting")
    logger.info("=" * 60)

    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        # Initialize sensors
        sensor_init_success = initialize_sensors()

        if not sensor_init_success:
            logger.warning(
                "Some sensors failed to initialize. Continuing with available sensors..."
            )

        if state["tof"] is None and state["ltr559"] is None:
            logger.error("All sensors failed to initialize. Exiting.")
            sys.exit(1)

        # Run main async loop
        logger.info("Starting main monitoring loop")
        asyncio.run(main())

    except KeyboardInterrupt:
        logger.info("Script interrupted by user")
    except (OSError, RuntimeError, ValueError, TypeError) as e:
        logger.error("Fatal error: %s", e, exc_info=True)
        sys.exit(1)
    finally:
        logger.info("Cleaning up resources...")
        cleanup_sensors()
        logger.info("Script terminated")


if __name__ == "__main__":
    run()
