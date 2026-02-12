import asyncio
import time
import sys
import signal
import numpy as np
from datetime import datetime
import VL53L1X  # distance sensor
from ltr559 import LTR559  # light & proximity sensor
import paho.mqtt.publish as publish
import os
from pathlib import Path
from dotenv import load_dotenv
import logging
from typing import Optional

# Configure logging
log_file = Path(__file__).parent / "fumehood.log"
handlers = [logging.StreamHandler()]

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
    logger.error(f".env file not found at {env_path}")
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
MEASUREMENT_INTERVAL = int(os.getenv("MEASUREMENT_INTERVAL", "10"))

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
logger.info(f"MQTT Server: {MQTT_SERVER}:{MQTT_PORT}, Path: {MQTT_PATH}")
logger.info(f"Lab ID: {LAB_ID}, Sublab ID: {SUBLAB_ID}")

# Global sensor objects
tof = None
ltr559 = None
shutdown_flag = False


def initialize_sensors() -> bool:
    """Initialize all sensors with error handling"""
    global tof, ltr559

    success = True

    # Initialize distance sensor (VL53L1X)
    try:
        logger.info(
            f"Initializing distance sensor on I2C bus {TOF_I2C_BUS}, address {hex(TOF_I2C_ADDRESS)}"
        )
        tof = VL53L1X.VL53L1X(i2c_bus=TOF_I2C_BUS, i2c_address=TOF_I2C_ADDRESS)
        tof.open()
        tof.start_ranging(TOF_RANGING_MODE)
        logger.info(f"Distance sensor initialized (ranging mode: {TOF_RANGING_MODE})")
    except Exception as e:
        logger.error(f"Failed to initialize distance sensor: {e}")
        tof = None
        success = False

    # Initialize light sensor (LTR559)
    try:
        logger.info("Initializing light sensor")
        ltr559 = LTR559()
        logger.info("Light sensor initialized")
    except Exception as e:
        logger.error(f"Failed to initialize light sensor: {e}")
        ltr559 = None
        success = False

    return success


def cleanup_sensors():
    """Clean up sensor resources"""
    global tof

    if tof is not None:
        try:
            tof.stop_ranging()
            tof.close()
            logger.info("Distance sensor cleaned up")
        except Exception as e:
            logger.warning(f"Error cleaning up distance sensor: {e}")

    # LTR559 doesn't require explicit cleanup
    logger.info("Sensors cleaned up")


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global shutdown_flag
    logger.info(f"Received signal {signum}. Shutting down gracefully...")
    shutdown_flag = True
    cleanup_sensors()
    sys.exit(0)


def read_distance_sensor() -> Optional[float]:
    """Read distance from TOF sensor with error handling"""
    global tof

    if tof is None:
        logger.warning("Distance sensor not initialized")
        return None

    try:
        distance = tof.get_distance()
        logger.debug(f"Distance reading: {distance} mm")
        return float(distance)
    except Exception as e:
        logger.error(f"Error reading distance sensor: {e}")
        return None


def read_light_sensor() -> Optional[float]:
    """Read light level from LTR559 sensor with error handling"""
    global ltr559

    if ltr559 is None:
        logger.warning("Light sensor not initialized")
        return None

    try:
        ltr559.update_sensor()
        lux = ltr559.get_lux()
        logger.debug(f"Light reading: {lux} lux")
        return float(lux)
    except Exception as e:
        logger.error(f"Error reading light sensor: {e}")
        return None


def validate_distance(distance: Optional[float]) -> bool:
    """Validate that distance reading is within acceptable range"""
    if distance is None:
        return False
    if distance < DISTANCE_MIN_MM or distance > DISTANCE_MAX_MM:
        logger.warning(
            f"Distance reading {distance} mm is outside valid range "
            f"({DISTANCE_MIN_MM}-{DISTANCE_MAX_MM} mm)"
        )
        return False
    return True


def validate_light(lux: Optional[float]) -> bool:
    """Validate that light level reading is within acceptable range"""
    if lux is None:
        return False
    if lux < LIGHT_MIN_LUX or lux > LIGHT_MAX_LUX:
        logger.warning(
            f"Light level reading {lux} lux is outside valid range "
            f"({LIGHT_MIN_LUX}-{LIGHT_MAX_LUX} lux)"
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
            logger.info(f"MQTT message published successfully")
            logger.debug(f"Payload: {msg_payload}")
            return True

        except ConnectionRefusedError:
            logger.error(
                f"MQTT connection refused (attempt {attempt}/{retry_count}): "
                f"Check if MQTT broker is running at {MQTT_SERVER}:{MQTT_PORT}"
            )
        except TimeoutError:
            logger.error(
                f"MQTT timeout (attempt {attempt}/{retry_count}): "
                f"Broker not responding at {MQTT_SERVER}:{MQTT_PORT}"
            )
        except Exception as e:
            logger.error(f"MQTT publish error (attempt {attempt}/{retry_count}): {e}")

        if attempt < retry_count:
            time.sleep(2)

    return False


async def main():
    """Main monitoring loop with error handling"""
    logger.info("Starting fumehood monitoring main loop")

    iteration = 0
    consecutive_errors = 0
    max_consecutive_errors = 5

    while not shutdown_flag:
        try:
            iteration += 1
            logger.debug(f"Starting measurement iteration {iteration}")

            time_send = datetime.now()

            # Read all sensors
            distance = read_distance_sensor()
            lux = read_light_sensor()
            airflow = 0.0  # Placeholder for future airflow sensor

            # Log readings
            logger.info(
                f"Readings at {time_send.strftime('%Y-%m-%d %H:%M:%S')}: "
                f"distance={distance if distance is not None else 'N/A'} mm, "
                f"light={lux if lux is not None else 'N/A'} lux, "
                f"airflow={airflow}"
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
                        f"Failed to publish MQTT message "
                        f"({consecutive_errors}/{max_consecutive_errors})"
                    )
            else:
                logger.warning(
                    "No valid sensor readings available, skipping MQTT publish"
                )
                consecutive_errors += 1

            # Check for too many consecutive errors
            if consecutive_errors >= max_consecutive_errors:
                logger.error(
                    f"Too many consecutive failures ({consecutive_errors}). "
                    "Check sensor connections and MQTT broker."
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
        except Exception as e:
            logger.error(f"Error in main loop: {e}", exc_info=True)
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

        if tof is None and ltr559 is None:
            logger.error("All sensors failed to initialize. Exiting.")
            sys.exit(1)

        # Run main async loop
        logger.info("Starting main monitoring loop")
        asyncio.run(main())

    except KeyboardInterrupt:
        logger.info("Script interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("Cleaning up resources...")
        cleanup_sensors()
        logger.info("Script terminated")


if __name__ == "__main__":
    run()
