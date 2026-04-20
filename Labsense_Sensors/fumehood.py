"""Fumehood sensor monitor.

Reads distance and ambient light sensors, publishes measurements to MQTT,
attempts sensor recovery on repeated zero-distance faults, and reboots the Pi
only when recovery fails.
"""

import asyncio
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime
from pathlib import Path
import signal
import statistics
import subprocess
import sys
import time
from typing import Any, Optional

from paho.mqtt import publish
from fumehood_helpers import (
    close_distance_sensor_instance,
    close_light_sensor_instance,
    load_fumehood_settings,
)

try:
    import VL53L1X  # type: ignore[reportMissingImports]  # distance sensor
except ImportError:  # pragma: no cover - depends on Raspberry Pi hardware image
    VL53L1X = None

try:
    from ltr559 import LTR559  # type: ignore[reportMissingImports]  # light sensor
except ImportError:  # pragma: no cover - depends on Raspberry Pi hardware image
    LTR559 = None

script_dir = Path(__file__).parent

try:
    SETTINGS = load_fumehood_settings(script_dir)
except FileNotFoundError as error:
    print(f"Error: {error}")
    sys.exit(1)

# Configure logging
log_file = script_dir / "fumehood.log"
log_retention_days = SETTINGS.log_retention_days
log_rotate_when = SETTINGS.log_rotate_when
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
MQTT_SERVER = SETTINGS.mqtt_server
MQTT_PATH = SETTINGS.mqtt_path
MQTT_PORT = SETTINGS.mqtt_port
MQTT_TIMEOUT = SETTINGS.mqtt_timeout

# Lab Configuration
LAB_ID = SETTINGS.lab_id
SUBLAB_ID = SETTINGS.sublab_id

# Sensor Configuration
TOF_I2C_BUS = SETTINGS.tof_i2c_bus
TOF_I2C_ADDRESS = SETTINGS.tof_i2c_address
TOF_RANGING_MODE = SETTINGS.tof_ranging_mode  # 1=Short, 2=Medium, 3=Long
TOF_TIMING_BUDGET_US = SETTINGS.tof_timing_budget_us
TOF_INTER_MEASUREMENT_MS = SETTINGS.tof_inter_measurement_ms

# Measurement Configuration
MEASUREMENT_INTERVAL = SETTINGS.measurement_interval
SENSOR_STABILIZE_DELAY_SECONDS = SETTINGS.sensor_stabilize_delay_seconds

DISTANCE_RETRY = SETTINGS.distance_retry
LIGHT_RETRY = SETTINGS.light_retry
RECOVERY = SETTINGS.recovery

# Backward-compatible aliases used throughout runtime logic.
DISTANCE_SAMPLE_COUNT = DISTANCE_RETRY.sample_count
DISTANCE_SAMPLE_DELAY_SECONDS = DISTANCE_RETRY.sample_delay_seconds
DISTANCE_ZERO_RETRY_COUNT = DISTANCE_RETRY.zero_retry_count
DISTANCE_ZERO_RETRY_DELAY_SECONDS = DISTANCE_RETRY.zero_retry_delay_seconds
DISTANCE_WARMUP_DISCARD_COUNT = DISTANCE_RETRY.warmup_discard_count
DISTANCE_WARMUP_DISCARD_DELAY_SECONDS = DISTANCE_RETRY.warmup_discard_delay_seconds

LIGHT_SAMPLE_COUNT = LIGHT_RETRY.sample_count
LIGHT_SAMPLE_DELAY_SECONDS = LIGHT_RETRY.sample_delay_seconds
LIGHT_ZERO_RETRY_COUNT = LIGHT_RETRY.zero_retry_count
LIGHT_ZERO_RETRY_DELAY_SECONDS = LIGHT_RETRY.zero_retry_delay_seconds
LIGHT_I2C_ERROR_RETRY_COUNT = LIGHT_RETRY.i2c_error_retry_count
LIGHT_I2C_ERROR_RETRY_DELAY_SECONDS = LIGHT_RETRY.i2c_error_retry_delay_seconds
LIGHT_WARMUP_DISCARD_COUNT = LIGHT_RETRY.warmup_discard_count
LIGHT_WARMUP_DISCARD_DELAY_SECONDS = LIGHT_RETRY.warmup_discard_delay_seconds

LIGHT_READ_ERROR_REINIT_THRESHOLD = RECOVERY.light_read_error_reinit_threshold
PROACTIVE_REINIT_INTERVAL_SECONDS = RECOVERY.proactive_reinit_interval_seconds
ZERO_DISTANCE_REBOOT_THRESHOLD = RECOVERY.zero_distance_reboot_threshold
ZERO_LIGHT_REINIT_THRESHOLD = RECOVERY.zero_light_reinit_threshold
IDENTICAL_LIGHT_REINIT_THRESHOLD = RECOVERY.identical_light_reinit_threshold
RECOVERY_FAILURE_BACKOFF_SECONDS = RECOVERY.failure_backoff_seconds
RECOVERY_CIRCUIT_BREAKER_THRESHOLD = RECOVERY.circuit_breaker_threshold
RECOVERY_CIRCUIT_BREAKER_WINDOW_SECONDS = RECOVERY.circuit_breaker_window_seconds
RECOVERY_EXIT_ON_CIRCUIT_BREAKER = RECOVERY.exit_on_circuit_breaker

# Sensor Validation Configuration
DISTANCE_MIN_MM = SETTINGS.distance_min_mm  # Minimum valid distance in mm
DISTANCE_MAX_MM = SETTINGS.distance_max_mm  # Maximum valid distance in mm
LIGHT_MIN_LUX = SETTINGS.light_min_lux  # Minimum valid light level in lux
LIGHT_MAX_LUX = SETTINGS.light_max_lux  # Maximum valid light level in lux

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
    "exit_code": 0,
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
            if state["tof"] is not None:
                close_distance_sensor_instance(state["tof"])
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
            if state["ltr559"] is not None:
                close_light_sensor_instance(state["ltr559"])
            logger.error("Failed to initialize light sensor: %s", e)
            state["ltr559"] = None
            success = False

    return success


def cleanup_sensors():
    """Clean up sensor resources"""
    if state["tof"] is not None:
        tof = state["tof"]
        try:
            tof.stop_ranging()
        except (OSError, RuntimeError, ValueError, TypeError) as e:
            logger.warning("Error stopping distance sensor ranging: %s", e)

        try:
            tof.close()
            logger.info("Distance sensor cleaned up")
        except (OSError, RuntimeError, ValueError, TypeError) as e:
            logger.warning("Error closing distance sensor: %s", e)
        finally:
            state["tof"] = None

    if state["ltr559"] is not None:
        close_light_sensor_instance(state["ltr559"])

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


def attempt_sensor_recovery(
    reason: str,
    recovery_failure_timestamps: list[float],
    recovery_backoff_until: float,
) -> tuple[bool, float]:
    """Attempt sensor recovery with backoff and circuit-breaker protection."""
    now = time.monotonic()
    if recovery_backoff_until > now:
        remaining_seconds = recovery_backoff_until - now
        logger.warning(
            "Skipping sensor recovery (%s) due to active backoff: %.1fs remaining",
            reason,
            remaining_seconds,
        )
        return False, recovery_backoff_until

    logger.warning("Sensor recovery trigger: %s", reason)
    if recover_sensors():
        recovery_failure_timestamps.clear()
        return True, 0.0

    failed_at = time.monotonic()
    window_seconds = max(RECOVERY_CIRCUIT_BREAKER_WINDOW_SECONDS, 0)
    if window_seconds > 0:
        recovery_failure_timestamps[:] = [
            timestamp
            for timestamp in recovery_failure_timestamps
            if failed_at - timestamp <= window_seconds
        ]
    else:
        recovery_failure_timestamps.clear()

    recovery_failure_timestamps.append(failed_at)

    next_backoff_until = 0.0
    if RECOVERY_FAILURE_BACKOFF_SECONDS > 0:
        next_backoff_until = failed_at + RECOVERY_FAILURE_BACKOFF_SECONDS
        logger.warning(
            "Recovery failed for '%s'. Applying backoff for %ss",
            reason,
            RECOVERY_FAILURE_BACKOFF_SECONDS,
        )

    if 0 < RECOVERY_CIRCUIT_BREAKER_THRESHOLD <= len(recovery_failure_timestamps):
        logger.critical(
            "Recovery circuit breaker opened after %s failed recoveries in %ss",
            len(recovery_failure_timestamps),
            window_seconds,
        )
        if RECOVERY_EXIT_ON_CIRCUIT_BREAKER:
            logger.critical(
                "Configured to exit on circuit breaker. Stopping process for service restart"
            )
            state["exit_code"] = 1
            state["shutdown_flag"] = True

    return False, next_backoff_until


async def main():  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
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
    recovery_backoff_until = 0.0
    recovery_failure_timestamps: list[float] = []

    def apply_recovery_success(
        *,
        reset_zero_distance: bool = False,
        reset_zero_light: bool = False,
    ) -> None:
        nonlocal last_recovery_monotonic
        nonlocal consecutive_zero_distance
        nonlocal consecutive_zero_light
        nonlocal consecutive_light_read_errors
        nonlocal consecutive_identical_light
        nonlocal last_light_value
        nonlocal consecutive_errors

        last_recovery_monotonic = time.monotonic()
        if reset_zero_distance:
            consecutive_zero_distance = 0
        if reset_zero_light:
            consecutive_zero_light = 0
        consecutive_light_read_errors = 0
        consecutive_identical_light = 0
        last_light_value = None
        consecutive_errors = 0

    def trigger_recovery(
        reason: str,
        *,
        reset_zero_distance: bool = False,
        reset_zero_light: bool = False,
    ) -> tuple[bool, bool]:
        nonlocal recovery_backoff_until

        recovered, recovery_backoff_until = attempt_sensor_recovery(
            reason,
            recovery_failure_timestamps,
            recovery_backoff_until,
        )

        if recovered:
            apply_recovery_success(
                reset_zero_distance=reset_zero_distance,
                reset_zero_light=reset_zero_light,
            )
            return True, False

        return False, bool(state["shutdown_flag"])

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
                    recovered, should_break = trigger_recovery(
                        "proactive re-initialization interval reached",
                        reset_zero_distance=True,
                        reset_zero_light=True,
                    )
                    if recovered:
                        continue
                    if should_break:
                        break
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
                if 0 < LIGHT_READ_ERROR_REINIT_THRESHOLD <= consecutive_light_read_errors:
                    recovered, should_break = trigger_recovery(
                        "repeated light sensor read failures",
                        reset_zero_light=True,
                    )
                    if recovered:
                        continue
                    if should_break:
                        break
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
                    recovered, should_break = trigger_recovery(
                        "repeated zero distance readings",
                        reset_zero_distance=True,
                    )
                    if recovered:
                        continue
                    if should_break:
                        break
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
                    recovered, should_break = trigger_recovery(
                        "repeated zero light readings",
                        reset_zero_light=True,
                    )
                    if recovered:
                        continue
                    if should_break:
                        break
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
                if 0 < IDENTICAL_LIGHT_REINIT_THRESHOLD <= consecutive_identical_light:
                    logger.warning(
                        "Reinitializing sensors due to %s identical light readings: %s lux",
                        consecutive_identical_light,
                        lux,
                    )
                    recovered, should_break = trigger_recovery(
                        "repeated identical light readings",
                        reset_zero_light=True,
                    )
                    if recovered:
                        continue
                    if should_break:
                        break
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

        if state["exit_code"] != 0:
            logger.error(
                "Exiting with status %s after recovery circuit breaker",
                state["exit_code"],
            )
            sys.exit(int(state["exit_code"]))

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
