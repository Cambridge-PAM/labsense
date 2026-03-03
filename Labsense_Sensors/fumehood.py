"""Fumehood sensor monitor.

Reads distance and ambient light sensors, publishes measurements to MQTT,
attempts sensor recovery on repeated zero-distance faults, and reboots the Pi
only when recovery fails.
"""

import asyncio
import time
import sys
import signal
import statistics
from datetime import datetime
import paho.mqtt.publish as publish
import os
import subprocess
from pathlib import Path
from dotenv import load_dotenv
import logging
from logging.handlers import TimedRotatingFileHandler
from typing import Any, Optional

try:
    import VL53L1X  # type: ignore[reportMissingImports]  # distance sensor
except ImportError:  # pragma: no cover - depends on Raspberry Pi hardware image
    VL53L1X = None

try:
    from ltr559 import LTR559  # type: ignore[reportMissingImports]  # light sensor
except ImportError:  # pragma: no cover - depends on Raspberry Pi hardware image
    LTR559 = None

# Load environment variables from .env file
env_path = Path(__file__).parent / ".env"
if not env_path.exists():
    print(f"Error: .env file not found at {env_path}")
    sys.exit(1)

load_dotenv(dotenv_path=env_path)

# Configure logging
log_file = Path(__file__).parent / "fumehood.log"
log_retention_days = int(os.getenv("FUMEHOOD_LOG_RETENTION_DAYS", "7"))
log_rotate_when = os.getenv("FUMEHOOD_LOG_ROTATE_WHEN", "midnight")
handlers: list[logging.Handler] = [logging.StreamHandler()]

try:
    handlers.append(
        TimedRotatingFileHandler(
            filename=log_file,
            when=log_rotate_when,
            interval=1,
            backupCount=max(log_retention_days, 0),
            encoding="utf-8",
        )
    )
except (PermissionError, OSError) as error:
    print(
        f"Warning: Cannot configure log file rotation at {log_file}: {error}. "
        "Logging to console only."
    )

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=handlers,
)
logger = logging.getLogger(__name__)

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
TOF_TIMING_BUDGET_US = int(os.getenv("TOF_TIMING_BUDGET_US", "0"))
TOF_INTER_MEASUREMENT_MS = int(os.getenv("TOF_INTER_MEASUREMENT_MS", "0"))

# Measurement Configuration
MEASUREMENT_INTERVAL = int(os.getenv("MEASUREMENT_INTERVAL", "30"))
SENSOR_STABILIZE_DELAY_SECONDS = float(
    os.getenv("SENSOR_STABILIZE_DELAY_SECONDS", "1.0")
)
DISTANCE_SAMPLE_COUNT = int(os.getenv("DISTANCE_SAMPLE_COUNT", "3"))
DISTANCE_SAMPLE_DELAY_SECONDS = float(
    os.getenv("DISTANCE_SAMPLE_DELAY_SECONDS", "0.03")
)
DISTANCE_ZERO_RETRY_COUNT = int(os.getenv("DISTANCE_ZERO_RETRY_COUNT", "1"))
DISTANCE_ZERO_RETRY_DELAY_SECONDS = float(
    os.getenv("DISTANCE_ZERO_RETRY_DELAY_SECONDS", "0.05")
)
DISTANCE_WARMUP_DISCARD_COUNT = int(os.getenv("DISTANCE_WARMUP_DISCARD_COUNT", "2"))
DISTANCE_WARMUP_DISCARD_DELAY_SECONDS = float(
    os.getenv("DISTANCE_WARMUP_DISCARD_DELAY_SECONDS", "0.05")
)
LIGHT_SAMPLE_COUNT = int(os.getenv("LIGHT_SAMPLE_COUNT", "3"))
LIGHT_SAMPLE_DELAY_SECONDS = float(os.getenv("LIGHT_SAMPLE_DELAY_SECONDS", "0.03"))
LIGHT_ZERO_RETRY_COUNT = int(os.getenv("LIGHT_ZERO_RETRY_COUNT", "1"))
LIGHT_ZERO_RETRY_DELAY_SECONDS = float(
    os.getenv("LIGHT_ZERO_RETRY_DELAY_SECONDS", "0.05")
)
LIGHT_I2C_ERROR_RETRY_COUNT = int(os.getenv("LIGHT_I2C_ERROR_RETRY_COUNT", "2"))
LIGHT_I2C_ERROR_RETRY_DELAY_SECONDS = float(
    os.getenv("LIGHT_I2C_ERROR_RETRY_DELAY_SECONDS", "0.1")
)
LIGHT_WARMUP_DISCARD_COUNT = int(os.getenv("LIGHT_WARMUP_DISCARD_COUNT", "2"))
LIGHT_WARMUP_DISCARD_DELAY_SECONDS = float(
    os.getenv("LIGHT_WARMUP_DISCARD_DELAY_SECONDS", "0.05")
)
LIGHT_READ_ERROR_REINIT_THRESHOLD = int(
    os.getenv("LIGHT_READ_ERROR_REINIT_THRESHOLD", "3")
)
PROACTIVE_REINIT_INTERVAL_SECONDS = int(
    os.getenv("PROACTIVE_REINIT_INTERVAL_SECONDS", "0")
)
ZERO_DISTANCE_REBOOT_THRESHOLD = int(os.getenv("ZERO_DISTANCE_REBOOT_THRESHOLD", "10"))
ZERO_LIGHT_REINIT_THRESHOLD = int(os.getenv("ZERO_LIGHT_REINIT_THRESHOLD", "5"))
IDENTICAL_LIGHT_REINIT_THRESHOLD = int(
    os.getenv("IDENTICAL_LIGHT_REINIT_THRESHOLD", "5")
)

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
            if TOF_TIMING_BUDGET_US > 0 and TOF_INTER_MEASUREMENT_MS > 0:
                state["tof"].set_timing(TOF_TIMING_BUDGET_US, TOF_INTER_MEASUREMENT_MS)
                state["tof"].start_ranging(0)
                logger.info(
                    "Distance sensor explicit timing enabled (budget=%sus, inter=%sms)",
                    TOF_TIMING_BUDGET_US,
                    TOF_INTER_MEASUREMENT_MS,
                )
            else:
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

    sample_count = max(DISTANCE_SAMPLE_COUNT, 1)
    samples: list[float] = []

    try:
        for sample_index in range(1, sample_count + 1):
            distance = _read_distance_once_with_zero_retry(tof)
            if distance is not None:
                samples.append(distance)

            if sample_index < sample_count:
                time.sleep(DISTANCE_SAMPLE_DELAY_SECONDS)

        if not samples:
            return None

        non_zero_samples = [sample for sample in samples if sample != 0.0]
        if non_zero_samples:
            aggregated_distance = float(statistics.median(non_zero_samples))
        else:
            aggregated_distance = 0.0

        logger.debug(
            "Distance samples=%s, aggregated distance=%s mm",
            samples,
            aggregated_distance,
        )
        return aggregated_distance
    except (OSError, RuntimeError, ValueError, TypeError) as e:
        logger.error("Error reading distance sensor: %s", e)
        return None


def _read_distance_once_with_zero_retry(tof) -> Optional[float]:
    """Read one distance sample, retrying transient zero values."""
    attempts = 1 + max(DISTANCE_ZERO_RETRY_COUNT, 0)
    for attempt in range(1, attempts + 1):
        distance = float(tof.get_distance())
        logger.debug(
            "Distance raw reading (attempt %s/%s): %s mm", attempt, attempts, distance
        )

        if distance != 0.0:
            return distance

        if attempt < attempts:
            logger.debug(
                "Zero distance reading; retrying in %s seconds",
                DISTANCE_ZERO_RETRY_DELAY_SECONDS,
            )
            time.sleep(DISTANCE_ZERO_RETRY_DELAY_SECONDS)

    return 0.0


def stabilize_sensors(context: str = "startup"):
    """Wait briefly after sensor (re)initialization to allow stable readings."""
    if SENSOR_STABILIZE_DELAY_SECONDS <= 0:
        return
    logger.info(
        "Waiting %s seconds for sensor stabilization (%s)",
        SENSOR_STABILIZE_DELAY_SECONDS,
        context,
    )
    time.sleep(SENSOR_STABILIZE_DELAY_SECONDS)


def warmup_sensors(context: str = "startup"):
    """Discard initial readings after init/recovery to reduce startup transients."""
    if state["tof"] is not None and DISTANCE_WARMUP_DISCARD_COUNT > 0:
        logger.info(
            "Discarding first %s distance readings for warm-up (%s)",
            DISTANCE_WARMUP_DISCARD_COUNT,
            context,
        )

        for attempt in range(1, DISTANCE_WARMUP_DISCARD_COUNT + 1):
            warmup_distance = read_distance_sensor()
            logger.debug(
                "Warm-up distance reading %s/%s: %s mm",
                attempt,
                DISTANCE_WARMUP_DISCARD_COUNT,
                warmup_distance,
            )

            if attempt < DISTANCE_WARMUP_DISCARD_COUNT:
                time.sleep(DISTANCE_WARMUP_DISCARD_DELAY_SECONDS)

    if state["ltr559"] is not None and LIGHT_WARMUP_DISCARD_COUNT > 0:
        logger.info(
            "Discarding first %s light readings for warm-up (%s)",
            LIGHT_WARMUP_DISCARD_COUNT,
            context,
        )

        for attempt in range(1, LIGHT_WARMUP_DISCARD_COUNT + 1):
            warmup_lux = read_light_sensor()
            logger.debug(
                "Warm-up light reading %s/%s: %s lux",
                attempt,
                LIGHT_WARMUP_DISCARD_COUNT,
                warmup_lux,
            )

            if attempt < LIGHT_WARMUP_DISCARD_COUNT:
                time.sleep(LIGHT_WARMUP_DISCARD_DELAY_SECONDS)


def read_light_sensor() -> Optional[float]:
    """Read light level from LTR559 sensor with error handling"""
    ltr559 = state["ltr559"]
    if ltr559 is None:
        logger.warning("Light sensor not initialized")
        return None

    sample_count = max(LIGHT_SAMPLE_COUNT, 1)
    samples: list[float] = []

    try:
        for sample_index in range(1, sample_count + 1):
            lux = _read_light_once_with_zero_retry(ltr559)
            if lux is not None:
                samples.append(lux)

            if sample_index < sample_count:
                time.sleep(LIGHT_SAMPLE_DELAY_SECONDS)

        if not samples:
            return None

        non_zero_samples = [sample for sample in samples if sample != 0.0]
        if non_zero_samples:
            aggregated_lux = float(statistics.median(non_zero_samples))
        else:
            aggregated_lux = 0.0

        logger.debug("Light samples=%s, aggregated lux=%s", samples, aggregated_lux)
        return aggregated_lux
    except (OSError, RuntimeError, ValueError, TypeError) as e:
        logger.error("Error reading light sensor: %s", e)
        return None


def _read_light_once_with_zero_retry(ltr559) -> Optional[float]:
    """Read one light sample, retrying transient zero values."""
    attempts = 1 + max(LIGHT_ZERO_RETRY_COUNT, 0)
    i2c_attempts = 1 + max(LIGHT_I2C_ERROR_RETRY_COUNT, 0)

    for attempt in range(1, attempts + 1):
        lux: Optional[float] = None

        for i2c_attempt in range(1, i2c_attempts + 1):
            try:
                ltr559.update_sensor()
                lux = float(ltr559.get_lux())
                break
            except OSError as error:
                if _is_remote_i2c_error(error) and i2c_attempt < i2c_attempts:
                    logger.warning(
                        "Light I2C read error (attempt %s/%s): %s. Retrying in %s seconds",
                        i2c_attempt,
                        i2c_attempts,
                        error,
                        LIGHT_I2C_ERROR_RETRY_DELAY_SECONDS,
                    )
                    time.sleep(LIGHT_I2C_ERROR_RETRY_DELAY_SECONDS)
                    continue
                raise

        if lux is None:
            return None

        logger.debug(
            "Light raw reading (attempt %s/%s): %s lux", attempt, attempts, lux
        )

        if lux != 0.0:
            return lux

        if attempt < attempts:
            logger.debug(
                "Zero light reading; retrying in %s seconds",
                LIGHT_ZERO_RETRY_DELAY_SECONDS,
            )
            time.sleep(LIGHT_ZERO_RETRY_DELAY_SECONDS)

    return 0.0


def _is_remote_i2c_error(error: OSError) -> bool:
    """Check whether an OSError corresponds to transient I2C remote I/O failures."""
    return error.errno == 121 or "Remote I/O error" in str(error)


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
            logger.error("MQTT connection refused: Check if MQTT broker is running")
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
        stabilize_sensors("recovery")
        warmup_sensors("recovery")
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
    consecutive_light_read_errors = 0
    consecutive_identical_light = 0
    last_light_value: Optional[float] = None
    last_recovery_monotonic = time.monotonic()

    while not state["shutdown_flag"]:
        try:
            iteration += 1
            logger.debug("Starting measurement iteration %s", iteration)

            if PROACTIVE_REINIT_INTERVAL_SECONDS > 0:
                elapsed_since_recovery = time.monotonic() - last_recovery_monotonic
                if elapsed_since_recovery >= PROACTIVE_REINIT_INTERVAL_SECONDS:
                    logger.info(
                        "Proactive sensor re-initialization triggered after %s seconds",
                        PROACTIVE_REINIT_INTERVAL_SECONDS,
                    )
                    if recover_sensors():
                        last_recovery_monotonic = time.monotonic()
                        consecutive_zero_distance = 0
                        consecutive_zero_light = 0
                        consecutive_light_read_errors = 0
                        consecutive_identical_light = 0
                        last_light_value = None
                        consecutive_errors = 0
                        continue
                    logger.warning("Proactive sensor re-initialization failed")

            time_send = datetime.now()

            # Read all sensors
            distance = read_distance_sensor()
            lux = read_light_sensor()
            airflow = 0.0  # Placeholder for future airflow sensor

            if lux is None:
                consecutive_light_read_errors += 1
                logger.warning(
                    "Light sensor read failed (%s/%s)",
                    consecutive_light_read_errors,
                    LIGHT_READ_ERROR_REINIT_THRESHOLD,
                )
                if (
                    LIGHT_READ_ERROR_REINIT_THRESHOLD > 0
                    and consecutive_light_read_errors
                    >= LIGHT_READ_ERROR_REINIT_THRESHOLD
                ):
                    if recover_sensors():
                        last_recovery_monotonic = time.monotonic()
                        consecutive_zero_light = 0
                        consecutive_light_read_errors = 0
                        consecutive_identical_light = 0
                        last_light_value = None
                        consecutive_errors = 0
                        continue
                    logger.error(
                        "Repeated light sensor read failures detected and recovery failed"
                    )
            else:
                consecutive_light_read_errors = 0

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
                        last_recovery_monotonic = time.monotonic()
                        consecutive_zero_distance = 0
                        consecutive_light_read_errors = 0
                        consecutive_identical_light = 0
                        last_light_value = None
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
                        last_recovery_monotonic = time.monotonic()
                        consecutive_zero_light = 0
                        consecutive_light_read_errors = 0
                        consecutive_identical_light = 0
                        last_light_value = None
                        consecutive_errors = 0
                        continue
                    logger.error(
                        "Repeated 0.0 lux readings detected and recovery failed"
                    )
            else:
                consecutive_zero_light = 0

            # Re-initialize sensors on repeated identical light readings
            if lux is not None and lux == last_light_value:
                consecutive_identical_light += 1
                logger.debug(
                    "Identical light reading detected: %s lux (%s/%s)",
                    lux,
                    consecutive_identical_light,
                    IDENTICAL_LIGHT_REINIT_THRESHOLD,
                )
                if (
                    IDENTICAL_LIGHT_REINIT_THRESHOLD > 0
                    and consecutive_identical_light >= IDENTICAL_LIGHT_REINIT_THRESHOLD
                ):
                    logger.warning(
                        "Reinitializing sensors due to %s identical light readings: %s lux",
                        consecutive_identical_light,
                        lux,
                    )
                    if recover_sensors():
                        last_recovery_monotonic = time.monotonic()
                        consecutive_zero_light = 0
                        consecutive_light_read_errors = 0
                        consecutive_identical_light = 0
                        last_light_value = None
                        consecutive_errors = 0
                        continue
                    logger.error(
                        "Repeated identical light readings detected and recovery failed"
                    )
            else:
                if lux is not None:
                    consecutive_identical_light = 1
                    last_light_value = lux
                else:
                    consecutive_identical_light = 0
                    last_light_value = None

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

        stabilize_sensors("startup")
        warmup_sensors("startup")

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
