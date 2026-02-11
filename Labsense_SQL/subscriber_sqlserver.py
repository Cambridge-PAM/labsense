import paho.mqtt.client as mqtt
import json
import pyodbc
import os
import sys
import signal
from pathlib import Path
from dotenv import load_dotenv
import logging
from typing import Optional, Dict, Any

# Configure logging
log_file = Path(__file__).parent / "subscriber_sqlserver.log"
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
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
    logger.info("Loaded environment variables from .env")
else:
    logger.warning(f".env file not found at {env_path}. Using default configuration.")

# MQTT Configuration
MQTT_SERVER = os.getenv("MQTT_SERVER", "10.253.179.46").strip()
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TIMEOUT = int(os.getenv("MQTT_TIMEOUT", "60"))
TOPICS = ["water", "fumehood"]

# SQL Server Configuration
SQL_SERVER = os.getenv("SQL_SERVER", "MSM-FPM-70203\\LABSENSE").strip()
SQL_DATABASE = os.getenv("SQL_DATABASE", "labsense").strip()
SQL_TRUSTED_CONNECTION = os.getenv("SQL_TRUSTED_CONNECTION", "yes").strip().lower()
SQL_ENCRYPTION = os.getenv("SQL_ENCRYPTION", "Optional").strip()
SQL_USER = os.getenv("SQL_USER", "").strip()
SQL_PASSWORD = os.getenv("SQL_PASSWORD", "").strip()

# Build connection string based on authentication method
if SQL_TRUSTED_CONNECTION == "yes":
    connection_string = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"Trusted_Connection=yes;"
        f"Encrypt={SQL_ENCRYPTION}"
    )
else:
    if not SQL_USER or not SQL_PASSWORD:
        logger.error(
            "SQL_USER and SQL_PASSWORD required when not using trusted connection"
        )
        sys.exit(1)
    connection_string = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD};"
        f"Encrypt={SQL_ENCRYPTION}"
    )

# Validate configuration
if not all([MQTT_SERVER, SQL_SERVER, SQL_DATABASE]):
    logger.error("Missing required configuration. Check .env file.")
    sys.exit(1)

logger.info(
    f"Configuration loaded: MQTT={MQTT_SERVER}, SQL Server={SQL_SERVER}, DB={SQL_DATABASE}"
)

# Global state
shutdown_flag = False
client = None
db_connection = None


def normalize_value(value: Any, default: float = 0.0) -> float:
    """Safely convert value to float with fallback"""
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        logger.warning(
            f"Could not convert value {value} to float, using default {default}"
        )
        return default


def init_database() -> bool:
    """Initialize database connection and create tables"""
    try:
        conn = pyodbc.connect(connection_string, timeout=30)
        cursor = conn.cursor()

        # Create water table if not exists
        cursor.execute(
            """
            IF NOT EXISTS (SELECT object_id FROM sys.objects WHERE object_id = OBJECT_ID(N'[water]') AND type = 'U')
            BEGIN
                CREATE TABLE water (
                    id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
                    LabId INTEGER,
                    SublabId INTEGER,
                    Water REAL,
                    Timestamp DATETIME
                )
            END
            """
        )

        # Create fumehood table if not exists
        cursor.execute(
            """
            IF NOT EXISTS (SELECT object_id FROM sys.objects WHERE object_id = OBJECT_ID(N'[fumehood]') AND type = 'U')
            BEGIN
                CREATE TABLE fumehood (
                    id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
                    LabId INTEGER,
                    SublabId INTEGER,
                    Distance REAL,
                    Light REAL,
                    Airflow REAL,
                    Timestamp DATETIME
                )
            END
            """
        )

        conn.commit()
        conn.close()
        logger.info("Database tables initialized successfully")
        return True

    except pyodbc.Error as e:
        logger.error(f"SQL Server database error during initialization: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error initializing database: {e}")
        return False


def insert_sql_water(lab_id: int, sublab_id: int, water: float, timestamp: str) -> bool:
    """Insert water data into SQL Server with error handling"""
    water = normalize_value(water, 0.0)

    try:
        connection = pyodbc.connect(connection_string, timeout=30)
        cursor = connection.cursor()

        cursor.execute(
            """
            INSERT INTO water (LabId, SublabId, Water, Timestamp)
            VALUES (?, ?, ?, ?)
            """,
            (lab_id, sublab_id, water, timestamp),
        )

        connection.commit()
        connection.close()
        logger.info(f"Water data inserted: LabId={lab_id}, Water={water:.3f}L")
        return True

    except pyodbc.Error as e:
        logger.error(f"SQL Server error inserting water data: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error inserting water data: {e}")
        return False


def insert_sql_fumehood(
    lab_id: int,
    sublab_id: int,
    distance: float,
    light: float,
    airflow: float,
    timestamp: str,
) -> bool:
    """Insert fumehood data into SQL Server with error handling"""
    distance = normalize_value(distance, 0.0)
    light = normalize_value(light, 0.0)
    airflow = normalize_value(airflow, 0.0)

    try:
        connection = pyodbc.connect(connection_string, timeout=30)
        cursor = connection.cursor()

        cursor.execute(
            """
            INSERT INTO fumehood (LabId, SublabId, Distance, Light, Airflow, Timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (lab_id, sublab_id, distance, light, airflow, timestamp),
        )

        connection.commit()
        connection.close()
        logger.info(
            f"Fumehood data inserted: LabId={lab_id}, Distance={distance}mm, Light={light}lux"
        )
        return True

    except pyodbc.Error as e:
        logger.error(f"SQL Server error inserting fumehood data: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error inserting fumehood data: {e}")
        return False


def on_connect(client, userdata, flags, rc):
    """MQTT callback when client connects to broker"""
    if rc == 0:
        logger.info("Connected to MQTT broker successfully")
        # Subscribe to topics
        for topic in TOPICS:
            try:
                client.subscribe(topic)
                logger.info(f"Subscribed to topic: {topic}")
            except Exception as e:
                logger.error(f"Failed to subscribe to topic {topic}: {e}")
    else:
        logger.error(f"Failed to connect to MQTT broker. Result code: {rc}")
        mqtt_error_strings = {
            1: "Connection refused - incorrect protocol version",
            2: "Connection refused - invalid client identifier",
            3: "Connection refused - server unavailable",
            4: "Connection refused - bad username or password",
            5: "Connection refused - not authorised",
        }
        error_msg = mqtt_error_strings.get(rc, "Unknown error")
        logger.error(f"Connection error details: {error_msg}")


def on_disconnect(client, userdata, rc):
    """MQTT callback when client disconnects from broker"""
    if rc != 0:
        logger.warning(
            f"Unexpected disconnection from MQTT broker (code: {rc}). Will auto-reconnect..."
        )
    else:
        logger.info("Disconnected from MQTT broker")


def on_message(client, userdata, msg):
    """MQTT callback when message is received"""
    try:
        # Decode message
        message_str = msg.payload.decode("utf-8")
        logger.debug(f"Received message on topic {msg.topic}: {message_str}")

        # Convert single quotes to double quotes for JSON parsing
        message_str = message_str.replace("'", '"')

        # Parse JSON
        try:
            data = json.loads(message_str)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON message on topic {msg.topic}: {e}")
            logger.debug(f"Failed message content: {message_str}")
            return

        # Extract common fields
        lab_id = data.get("labId")
        sublab_id = data.get("sublabId")
        timestamp = data.get("measureTimestamp")
        sensor_readings = data.get("sensorReadings")

        # Validate required fields
        if not all(
            [lab_id is not None, sublab_id is not None, timestamp, sensor_readings]
        ):
            logger.warning(
                f"Missing required fields in message on topic {msg.topic}. "
                f"labId={lab_id}, sublabId={sublab_id}, timestamp={timestamp}"
            )
            return

        # Process water sensor data
        if "water" in sensor_readings:
            try:
                water = sensor_readings.get("water")
                water_litres = normalize_value(water, 0.0) / 1000
                logger.debug(f"Water reading: {water_litres:.3f}L")
                insert_sql_water(lab_id, sublab_id, water_litres, timestamp)
            except Exception as e:
                logger.error(f"Error processing water data: {e}")

        # Process fumehood sensor data
        if "fumehood" in sensor_readings:
            try:
                fumehood_data = sensor_readings.get("fumehood")
                if isinstance(fumehood_data, dict):
                    distance = fumehood_data.get("distance")
                    light = fumehood_data.get("light")
                    airflow = fumehood_data.get("airflow")
                    logger.debug(
                        f"Fumehood readings: distance={distance}mm, light={light}lux, airflow={airflow}"
                    )
                    insert_sql_fumehood(
                        lab_id, sublab_id, distance, light, airflow, timestamp
                    )
                else:
                    logger.warning(
                        f"Fumehood data is not a dictionary: {fumehood_data}"
                    )
            except Exception as e:
                logger.error(f"Error processing fumehood data: {e}")

    except Exception as e:
        logger.error(f"Unexpected error processing message: {e}", exc_info=True)


def on_log(client, userdata, level, buf):
    """MQTT callback for logging"""
    if level == mqtt.MQTT_LOG_ERR:
        logger.error(f"MQTT Error: {buf}")
    elif level == mqtt.MQTT_LOG_WARNING:
        logger.warning(f"MQTT Warning: {buf}")


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global shutdown_flag, client
    logger.info(f"Received signal {signum}. Shutting down gracefully...")
    shutdown_flag = True
    if client:
        client.loop_stop()
        client.disconnect()
    sys.exit(0)


def main():
    """Main function to start MQTT subscriber"""
    global client, shutdown_flag

    logger.info("=" * 60)
    logger.info("MQTT SQL Server Subscriber Starting")
    logger.info("=" * 60)

    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Initialize database
    if not init_database():
        logger.error("Failed to initialize database. Exiting.")
        sys.exit(1)

    # Create MQTT client
    try:
        client = mqtt.Client()
        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        client.on_message = on_message
        client.on_log = on_log

        # Set keep alive
        client.loop_start()

        # Connect to MQTT broker
        logger.info(f"Connecting to MQTT broker at {MQTT_SERVER}:{MQTT_PORT}...")
        try:
            client.connect(MQTT_SERVER, MQTT_PORT, MQTT_TIMEOUT)
        except Exception as e:
            logger.error(f"Failed to connect to MQTT broker: {e}")
            sys.exit(1)

        # Keep running until shutdown signal
        try:
            while not shutdown_flag:
                import time

                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
        finally:
            client.loop_stop()
            client.disconnect()
            logger.info("MQTT subscriber stopped")

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
