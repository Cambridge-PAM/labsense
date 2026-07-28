"""MQTT subscriber that listens for sensor messages and writes readings to SQL Server."""

import paho.mqtt.client as mqtt
import json
import pyodbc
import os
import sys
import signal
from pathlib import Path
from dotenv import load_dotenv
import logging
from typing import Any

# Load environment variables from Labsense_SQL/.env
env_path = Path(__file__).parent / ".env"
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
else:
    print(f"Warning: .env file not found at {env_path}. Using default configuration.")

# Configure logging
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
log_file = Path(__file__).parent / "subscriber_sqlserver.log"
handlers = [logging.StreamHandler()]

try:
    handlers.append(logging.FileHandler(log_file))
except PermissionError:
    print(f"Warning: Cannot write to log file {log_file}. Logging to console only.")

logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=handlers,
)
logger = logging.getLogger(__name__)
logger.info("Loaded environment variables from %s", env_path)

# MQTT Configuration
MQTT_SERVER = os.getenv("MQTT_SERVER", "10.253.179.46").strip()
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TIMEOUT = int(os.getenv("MQTT_TIMEOUT", "60"))
TOPICS = ["water", "fumehood", "sen66"]

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

        # Create sen66 table if not exists
        cursor.execute(
            """
            IF NOT EXISTS (SELECT object_id FROM sys.objects WHERE object_id = OBJECT_ID(N'[sen66]') AND type = 'U')
            BEGIN
                CREATE TABLE sen66 (
                    id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
                    LabId INTEGER,
                    SublabId INTEGER,
                    Temperature REAL,
                    Humidity REAL,
                    Co2 REAL,
                    Voc REAL,
                    Nox REAL,
                    Pm1 REAL,
                    Pm25 REAL,
                    Pm4 REAL,
                    Pm10 REAL,
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


def insert_sql_sen66(
    lab_id: int,
    sublab_id: int,
    temperature: float,
    humidity: float,
    co2: float,
    voc: float,
    nox: float,
    pm1: float,
    pm25: float,
    pm4: float,
    pm10: float,
    timestamp: str,
) -> bool:
    """Insert SEN66 data into SQL Server with error handling."""
    temperature = normalize_value(temperature, 0.0)
    humidity = normalize_value(humidity, 0.0)
    co2 = normalize_value(co2, 0.0)
    voc = normalize_value(voc, 0.0)
    nox = normalize_value(nox, 0.0)
    pm1 = normalize_value(pm1, 0.0)
    pm25 = normalize_value(pm25, 0.0)
    pm4 = normalize_value(pm4, 0.0)
    pm10 = normalize_value(pm10, 0.0)

    try:
        connection = pyodbc.connect(connection_string, timeout=30)
        cursor = connection.cursor()

        cursor.execute(
            """
            INSERT INTO sen66 (
                LabId, SublabId, Temperature, Humidity, Co2, Voc, Nox, Pm1, Pm25, Pm4, Pm10, Timestamp
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                lab_id,
                sublab_id,
                temperature,
                humidity,
                co2,
                voc,
                nox,
                pm1,
                pm25,
                pm4,
                pm10,
                timestamp,
            ),
        )

        connection.commit()
        connection.close()
        logger.info(
            "SEN66 data inserted: LabId=%s, temp=%s C, humidity=%s %%RH, co2=%s ppm, voc=%s, nox=%s",
            lab_id,
            temperature,
            humidity,
            co2,
            voc,
            nox,
        )
        return True

    except pyodbc.Error as e:
        logger.error(f"SQL Server error inserting SEN66 data: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error inserting SEN66 data: {e}")
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
        ip_address = data.get("ipAddress", "unknown")
        lab_id = data.get("labId")
        sublab_id = data.get("sublabId")
        timestamp = data.get("measureTimestamp")
        sensor_readings = data.get("sensorReadings")

        logger.debug(
            "Received message from ipAddress=%s on topic %s: %s",
            ip_address,
            msg.topic,
            message_str,
        )

        # Validate required fields
        if not all(
            [lab_id is not None, sublab_id is not None, timestamp, sensor_readings]
        ):
            logger.warning(
                "Missing required fields in message from ipAddress=%s on topic %s. "
                "labId=%s, sublabId=%s, timestamp=%s",
                ip_address,
                msg.topic,
                lab_id,
                sublab_id,
                timestamp,
            )
            return

        # Process water sensor data
        if "water" in sensor_readings:
            try:
                water = sensor_readings.get("water")
                water_litres = normalize_value(water, 0.0) / 1000
                logger.debug(
                    "Water reading from ipAddress=%s: %.3fL",
                    ip_address,
                    water_litres,
                )
                insert_sql_water(lab_id, sublab_id, water_litres, timestamp)
            except Exception as e:
                logger.error(
                    "Error processing water data from ipAddress=%s: %s",
                    ip_address,
                    e,
                )

        # Process fumehood sensor data
        if "fumehood" in sensor_readings:
            try:
                fumehood_data = sensor_readings.get("fumehood")
                if isinstance(fumehood_data, dict):
                    distance = fumehood_data.get("distance")
                    light = fumehood_data.get("light")
                    airflow = fumehood_data.get("airflow")
                    logger.debug(
                        "Fumehood readings from ipAddress=%s: distance=%smm, light=%slux, airflow=%s",
                        ip_address,
                        distance,
                        light,
                        airflow,
                    )
                    insert_sql_fumehood(
                        lab_id, sublab_id, distance, light, airflow, timestamp
                    )
                else:
                    logger.warning(
                        "Fumehood data from ipAddress=%s is not a dictionary: %s",
                        ip_address,
                        fumehood_data,
                    )
            except Exception as e:
                logger.error(
                    "Error processing fumehood data from ipAddress=%s: %s",
                    ip_address,
                    e,
                )

        # Process SEN66 sensor data
        if "sen66" in sensor_readings:
            try:
                sen66_data = sensor_readings.get("sen66")
                if isinstance(sen66_data, dict):
                    temperature = sen66_data.get("temperature")
                    humidity = sen66_data.get("humidity")
                    co2 = sen66_data.get("co2")
                    voc = sen66_data.get("voc")
                    nox = sen66_data.get("nox")
                    pm1 = sen66_data.get("pm1")
                    pm25 = sen66_data.get("pm25")
                    pm4 = sen66_data.get("pm4")
                    pm10 = sen66_data.get("pm10")

                    logger.debug(
                        "SEN66 readings from ipAddress=%s: temp=%s C, humidity=%s %%RH, co2=%s ppm, voc=%s, nox=%s",
                        ip_address,
                        temperature,
                        humidity,
                        co2,
                        voc,
                        nox,
                    )
                    insert_sql_sen66(
                        lab_id,
                        sublab_id,
                        temperature,
                        humidity,
                        co2,
                        voc,
                        nox,
                        pm1,
                        pm25,
                        pm4,
                        pm10,
                        timestamp,
                    )
                else:
                    logger.warning(
                        "SEN66 data from ipAddress=%s is not a dictionary: %s",
                        ip_address,
                        sen66_data,
                    )
            except Exception as e:
                logger.error(
                    "Error processing SEN66 data from ipAddress=%s: %s",
                    ip_address,
                    e,
                )

    except Exception as e:
        logger.error(f"Unexpected error processing message: {e}", exc_info=True)


def on_log(client, userdata, level, buf):
    """MQTT callback for logging"""
    if level == mqtt.MQTT_LOG_ERR:
        logger.error(f"MQTT Error: {buf}")
    elif level == mqtt.MQTT_LOG_WARNING:
        logger.warning(f"MQTT Warning: {buf}")


def signal_handler(signum, frame):  # pylint: disable=unused-argument
    """Handle shutdown signals gracefully"""
    global shutdown_flag  # pylint: disable=global-statement
    logger.info(f"Received signal {signum}. Shutting down gracefully...")
    shutdown_flag = True
    if client:
        client.loop_stop()
        client.disconnect()
    sys.exit(0)


def main():
    """Main function to start MQTT subscriber"""
    global client  # pylint: disable=global-statement

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
