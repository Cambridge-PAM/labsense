import asyncio
import time
import schedule
import numpy as np
import threading
from datetime import datetime
import RPi.GPIO as GPIO
import paho.mqtt.publish as publish
import os
import sys
import signal
from pathlib import Path
from dotenv import load_dotenv
import logging
from typing import Optional

# Configure logging
log_file = Path(__file__).parent / "water-2taps.log"
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

# GPIO Configuration
FLOW_SENSOR_GPIO_1 = int(os.getenv("FLOW_SENSOR_GPIO_1", "4"))
FLOW_SENSOR_GPIO_2 = int(os.getenv("FLOW_SENSOR_GPIO_2", "17"))
LED_GPIO = int(os.getenv("LED_GPIO", "2"))
FLOW_RATE_FACTOR = float(os.getenv("FLOW_RATE_FACTOR", "5"))

# MQTT Configuration
MQTT_SERVER = os.getenv("MQTT_SERVER", "").strip()
MQTT_PATH = os.getenv("MQTT_PATH", "water").strip()
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TIMEOUT = int(os.getenv("MQTT_TIMEOUT", "10"))

# Lab Configuration
LAB_ID = int(os.getenv("LAB_ID", "1"))
SUBLAB_ID = int(os.getenv("SUBLAB_ID", "3"))

# Measurement Configuration
MEASUREMENT_INTERVAL = int(os.getenv("MEASUREMENT_INTERVAL", "5"))
PUBLISH_DELAY = int(os.getenv("PUBLISH_DELAY", "10"))

# Validate required configuration
if not MQTT_SERVER:
    logger.error("MQTT_SERVER not configured in .env file")
    sys.exit(1)

logger.info("Configuration loaded successfully")
logger.info(f"MQTT Server: {MQTT_SERVER}:{MQTT_PORT}, Path: {MQTT_PATH}")
logger.info(f"Lab ID: {LAB_ID}, Sublab ID: {SUBLAB_ID}")

# Thread-safe counter using threading.Lock
count_lock = threading.Lock()
count = 0
temp_volume_val = 0.0
total_volume_val = 0.0
shutdown_flag = threading.Event()

# GPIO initialization flag
gpio_initialized = False


def initialize_gpio() -> bool:
    """Initialize GPIO with error handling"""
    global gpio_initialized
    try:
        GPIO.setmode(GPIO.BCM)
        GPIO.setup(FLOW_SENSOR_GPIO_1, GPIO.IN, pull_up_down=GPIO.PUD_UP)
        GPIO.setup(FLOW_SENSOR_GPIO_2, GPIO.IN, pull_up_down=GPIO.PUD_UP)
        GPIO.setup(LED_GPIO, GPIO.OUT)
        GPIO.setwarnings(False)
        gpio_initialized = True
        logger.info("GPIO initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize GPIO: {e}")
        return False


def cleanup_gpio():
    """Clean up GPIO resources"""
    global gpio_initialized
    if gpio_initialized:
        try:
            GPIO.cleanup()
            logger.info("GPIO cleaned up successfully")
        except Exception as e:
            logger.warning(f"Error cleaning up GPIO: {e}")
        finally:
            gpio_initialized = False


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info(f"Received signal {signum}. Shutting down gracefully...")
    shutdown_flag.set()
    cleanup_gpio()
    sys.exit(0)


def countPulse(channel):
    """Count water flow pulses with thread safety"""
    global count
    try:
        with count_lock:
            count = count + 1
    except Exception as e:
        logger.error(f"Error in countPulse: {e}")


def gpio_monitoring():
    """Monitor GPIO sensors with error handling"""
    global count, temp_volume_val

    logger.info("GPIO monitoring thread started")

    while not shutdown_flag.is_set():
        try:
            with count_lock:
                current_count = count
                count = 0

            time.sleep(1)

            # Calculate volume in mL
            vol = (1000 * current_count) / (FLOW_RATE_FACTOR * 60)

            temp_volume_val = vol

            if vol > 0:
                logger.debug(f"Current volume: {vol:.2f} mL")

        except Exception as e:
            logger.error(f"Error in gpio_monitoring: {e}")
            time.sleep(1)

    logger.info("GPIO monitoring thread stopped")


def start_gpio_monitoring() -> Optional[threading.Thread]:
    """Start the background GPIO monitoring thread"""
    try:
        # Add GPIO event detection
        GPIO.add_event_detect(
            FLOW_SENSOR_GPIO_1, GPIO.FALLING, callback=countPulse, bouncetime=10
        )
        GPIO.add_event_detect(
            FLOW_SENSOR_GPIO_2, GPIO.FALLING, callback=countPulse, bouncetime=10
        )

        # Start monitoring thread as daemon
        thread = threading.Thread(target=gpio_monitoring, daemon=True)
        thread.start()
        logger.info("GPIO monitoring thread started successfully")
        return thread
    except Exception as e:
        logger.error(f"Failed to start GPIO monitoring: {e}")
        return None


def total_volume(interval: int) -> float:
    """Sum water volume over a set time period in seconds"""
    global total_volume_val, gpio_initialized

    if not gpio_initialized:
        logger.error("GPIO not initialized, cannot measure volume")
        return 0.0

    vol_arr = []

    try:
        GPIO.output(LED_GPIO, GPIO.HIGH)

        for i in range(interval):
            if shutdown_flag.is_set():
                break
            time.sleep(1)
            vol_arr.append(temp_volume_val)

        GPIO.output(LED_GPIO, GPIO.LOW)

        total_volume_val = np.sum(vol_arr)
        return total_volume_val

    except Exception as e:
        logger.error(f"Error measuring total volume: {e}")
        try:
            GPIO.output(LED_GPIO, GPIO.LOW)
        except:
            pass
        return 0.0


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
            logger.info(f"MQTT message published successfully: {msg_payload}")
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
    logger.info("Starting water monitoring main loop")

    iteration = 0
    consecutive_errors = 0
    max_consecutive_errors = 5

    while not shutdown_flag.is_set():
        try:
            iteration += 1
            logger.debug(f"Starting measurement iteration {iteration}")

            # Measure total volume over interval
            water_volume = total_volume(MEASUREMENT_INTERVAL)
            time_send = datetime.now()

            logger.info(f"Measured volume: {water_volume:.2f} mL")

            # Only send if water detected
            if water_volume > 0.0:
                msg_payload = str(
                    {
                        "labId": LAB_ID,
                        "sublabId": SUBLAB_ID,
                        "sensorReadings": {"water": float(water_volume)},
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
                logger.debug("No water flow detected, skipping MQTT publish")

            # Check for too many consecutive errors
            if consecutive_errors >= max_consecutive_errors:
                logger.error(
                    f"Too many consecutive MQTT failures ({consecutive_errors}). "
                    "Continuing monitoring but check MQTT broker connection."
                )
                consecutive_errors = 0  # Reset to avoid log spam

            # Wait before next measurement
            await asyncio.sleep(PUBLISH_DELAY)

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
    logger.info("Water Flow Monitoring Script Starting")
    logger.info("=" * 60)

    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        # Initialize GPIO
        if not initialize_gpio():
            logger.error("Failed to initialize GPIO. Exiting.")
            sys.exit(1)

        # Start GPIO monitoring thread
        monitoring_thread = start_gpio_monitoring()
        if not monitoring_thread:
            logger.error("Failed to start GPIO monitoring. Exiting.")
            cleanup_gpio()
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
        shutdown_flag.set()
        cleanup_gpio()
        logger.info("Script terminated")


if __name__ == "__main__":
    run()
