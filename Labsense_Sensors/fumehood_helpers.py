"""Helper utilities and configuration loader for fumehood monitoring."""

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


def _env_str(name: str, default: str) -> str:
    return os.getenv(name, default).strip()


def _env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def _env_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


def _env_bool(name: str, default: bool) -> bool:
    default_str = "true" if default else "false"
    return _env_str(name, default_str).lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class DistanceRetrySettings:
    sample_count: int
    sample_delay_seconds: float
    zero_retry_count: int
    zero_retry_delay_seconds: float
    warmup_discard_count: int
    warmup_discard_delay_seconds: float


@dataclass(frozen=True)
class LightRetrySettings:
    sample_count: int
    sample_delay_seconds: float
    zero_retry_count: int
    zero_retry_delay_seconds: float
    i2c_error_retry_count: int
    i2c_error_retry_delay_seconds: float
    warmup_discard_count: int
    warmup_discard_delay_seconds: float


@dataclass(frozen=True)
class RecoverySettings:
    light_read_error_reinit_threshold: int
    proactive_reinit_interval_seconds: int
    zero_distance_reboot_threshold: int
    zero_light_reinit_threshold: int
    identical_light_reinit_threshold: int
    failure_backoff_seconds: int
    circuit_breaker_threshold: int
    circuit_breaker_window_seconds: int
    exit_on_circuit_breaker: bool


@dataclass(frozen=True)
class FumehoodSettings:
    log_retention_days: int
    log_rotate_when: str
    mqtt_server: str
    mqtt_path: str
    mqtt_port: int
    mqtt_timeout: int
    lab_id: int
    sublab_id: int
    tof_i2c_bus: int
    tof_i2c_address: int
    tof_ranging_mode: int
    tof_timing_budget_us: int
    tof_inter_measurement_ms: int
    measurement_interval: int
    sensor_stabilize_delay_seconds: float
    distance_retry: DistanceRetrySettings
    light_retry: LightRetrySettings
    recovery: RecoverySettings
    distance_min_mm: int
    distance_max_mm: int
    light_min_lux: float
    light_max_lux: float


def load_fumehood_settings(script_dir: Path) -> FumehoodSettings:
    """Load .env-backed settings for fumehood monitoring."""
    env_path = script_dir / ".env"
    if not env_path.exists():
        raise FileNotFoundError(f".env file not found at {env_path}")

    load_dotenv(dotenv_path=env_path)

    distance_retry = DistanceRetrySettings(
        sample_count=_env_int("DISTANCE_SAMPLE_COUNT", 3),
        sample_delay_seconds=_env_float("DISTANCE_SAMPLE_DELAY_SECONDS", 0.03),
        zero_retry_count=_env_int("DISTANCE_ZERO_RETRY_COUNT", 1),
        zero_retry_delay_seconds=_env_float("DISTANCE_ZERO_RETRY_DELAY_SECONDS", 0.05),
        warmup_discard_count=_env_int("DISTANCE_WARMUP_DISCARD_COUNT", 2),
        warmup_discard_delay_seconds=_env_float(
            "DISTANCE_WARMUP_DISCARD_DELAY_SECONDS", 0.05
        ),
    )

    light_retry = LightRetrySettings(
        sample_count=_env_int("LIGHT_SAMPLE_COUNT", 3),
        sample_delay_seconds=_env_float("LIGHT_SAMPLE_DELAY_SECONDS", 0.03),
        zero_retry_count=_env_int("LIGHT_ZERO_RETRY_COUNT", 1),
        zero_retry_delay_seconds=_env_float("LIGHT_ZERO_RETRY_DELAY_SECONDS", 0.05),
        i2c_error_retry_count=_env_int("LIGHT_I2C_ERROR_RETRY_COUNT", 2),
        i2c_error_retry_delay_seconds=_env_float(
            "LIGHT_I2C_ERROR_RETRY_DELAY_SECONDS", 0.1
        ),
        warmup_discard_count=_env_int("LIGHT_WARMUP_DISCARD_COUNT", 2),
        warmup_discard_delay_seconds=_env_float(
            "LIGHT_WARMUP_DISCARD_DELAY_SECONDS", 0.05
        ),
    )

    recovery = RecoverySettings(
        light_read_error_reinit_threshold=_env_int(
            "LIGHT_READ_ERROR_REINIT_THRESHOLD", 3
        ),
        proactive_reinit_interval_seconds=_env_int(
            "PROACTIVE_REINIT_INTERVAL_SECONDS", 0
        ),
        zero_distance_reboot_threshold=_env_int("ZERO_DISTANCE_REBOOT_THRESHOLD", 10),
        zero_light_reinit_threshold=_env_int("ZERO_LIGHT_REINIT_THRESHOLD", 5),
        identical_light_reinit_threshold=_env_int(
            "IDENTICAL_LIGHT_REINIT_THRESHOLD", 5
        ),
        failure_backoff_seconds=_env_int("RECOVERY_FAILURE_BACKOFF_SECONDS", 120),
        circuit_breaker_threshold=_env_int("RECOVERY_CIRCUIT_BREAKER_THRESHOLD", 5),
        circuit_breaker_window_seconds=_env_int(
            "RECOVERY_CIRCUIT_BREAKER_WINDOW_SECONDS", 600
        ),
        exit_on_circuit_breaker=_env_bool("RECOVERY_EXIT_ON_CIRCUIT_BREAKER", True),
    )

    return FumehoodSettings(
        log_retention_days=_env_int("FUMEHOOD_LOG_RETENTION_DAYS", 7),
        log_rotate_when=_env_str("FUMEHOOD_LOG_ROTATE_WHEN", "midnight"),
        mqtt_server=_env_str("MQTT_SERVER", ""),
        mqtt_path=_env_str("MQTT_PATH_FUMEHOOD", "fumehood"),
        mqtt_port=_env_int("MQTT_PORT", 1883),
        mqtt_timeout=_env_int("MQTT_TIMEOUT", 10),
        lab_id=_env_int("LAB_ID", 1),
        sublab_id=_env_int("SUBLAB_ID", 3),
        tof_i2c_bus=_env_int("TOF_I2C_BUS", 1),
        tof_i2c_address=int(os.getenv("TOF_I2C_ADDRESS", "0x29"), 16),
        tof_ranging_mode=_env_int("TOF_RANGING_MODE", 1),
        tof_timing_budget_us=_env_int("TOF_TIMING_BUDGET_US", 0),
        tof_inter_measurement_ms=_env_int("TOF_INTER_MEASUREMENT_MS", 0),
        measurement_interval=_env_int("MEASUREMENT_INTERVAL", 30),
        sensor_stabilize_delay_seconds=_env_float(
            "SENSOR_STABILIZE_DELAY_SECONDS", 1.0
        ),
        distance_retry=distance_retry,
        light_retry=light_retry,
        recovery=recovery,
        distance_min_mm=_env_int("DISTANCE_MIN_MM", 0),
        distance_max_mm=_env_int("DISTANCE_MAX_MM", 4000),
        light_min_lux=_env_float("LIGHT_MIN_LUX", 0),
        light_max_lux=_env_float("LIGHT_MAX_LUX", 200000),
    )


def close_distance_sensor_instance(tof: Any) -> None:
    """Best-effort close of a distance sensor instance."""
    try:
        tof.stop_ranging()
    except (AttributeError, OSError, RuntimeError, ValueError, TypeError):
        pass

    try:
        tof.close()
    except (AttributeError, OSError, RuntimeError, ValueError, TypeError):
        pass


def close_light_sensor_instance(ltr559: Any) -> None:
    """Best-effort close of a light sensor instance and underlying I2C handle."""
    for method_name in ("close", "cleanup", "stop"):
        method = getattr(ltr559, method_name, None)
        if callable(method):
            try:
                method()
            except (OSError, RuntimeError, ValueError, TypeError):
                pass

    for attr_name in ("bus", "_bus", "i2c", "_i2c", "i2c_dev", "_i2c_dev"):
        handle = getattr(ltr559, attr_name, None)
        close_method = getattr(handle, "close", None)
        if callable(close_method):
            try:
                close_method()
            except (OSError, RuntimeError, ValueError, TypeError):
                pass
