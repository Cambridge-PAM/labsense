# LabSENSE

A laboratory IoT monitoring and analytics platform for chemistry lab environments. Collects real-time sensor data from Raspberry Pi devices, retrieves chemical inventory data from a cloud API, processes hazardous waste records, and generates interactive HTML dashboards visualising all data streams.

## Architecture

```
[Raspberry Pi 4 / Pico]  →  MQTT  →  Subscriber  →  SQL Server (labsense DB)
[ChemInventory API]      →  HTTP  →  CI scripts   →  SQL Server
[Waste Master.xlsx]      →  file  →  Waste module →  HTML dashboards (plots/)
[Balance (RS-232)]       →  serial → balance_ci   →  ChemInventory API update
```

## Modules

| Directory | Purpose |
|---|---|
| `Labsense_SQL/` | SQL Server data layer — inserts, queries, and HTML dashboard generation for chemicals, electricity, fumehood, and water |
| `Labsense_Sensors/` | Raspberry Pi sensor scripts — VL53L1X ToF (fumehood sash position), LTR559 light sensor, and GPIO water flow sensors |
| `ChemInventory/` | Ad-hoc scripts querying the ChemInventory REST API by CAS number or GHS hazard code |
| `Waste/` | Reads `Waste Master.xlsx`, proportionally allocates waste volumes across HP1–HP15 hazard codes, and generates per-HP dashboards |
| `Balance Comms/` | Scans a barcode, reads weight from a serial balance (Denver Instruments SI-2002), and updates ChemInventory via REST API |
| `RaspberryPi-4/` | Scheduled scripts pushing sensor streams (electricity, fumehood, GHS, order/waste lists) to Azure IoT Hub |
| `RaspberryPi-Pico/` | MicroPython firmware — WiFi, Azure IoT Hub connection, BME68x environmental sensor |
| `Labsense_SQLite/` | Legacy local SQLite equivalents of the SQL Server scripts |
| `Labsense_Excel/` | Legacy Excel-based order and waste update scripts using `openpyxl` |
| `tests/` | `pytest` unit tests for core processing logic |
| `plots/` | Generated HTML dashboards and matplotlib PNGs |
| `create_main_dashboard.py` | Generates the top-level HTML landing page linking all dashboards in `plots/` |

## Dependencies

Managed via Conda. Key packages:

- **Data processing**: `pandas`, `numpy`, `matplotlib`
- **Database**: `pyodbc` (SQL Server, ODBC Driver 18); `sqlite3` (legacy)
- **Messaging**: `paho-mqtt`, Azure IoT Hub SDK
- **APIs**: `requests`
- **Spreadsheets**: `openpyxl`
- **Configuration**: `python-dotenv`
- **Testing**: `pytest`
- **Hardware** *(Raspberry Pi only)*: `RPi.GPIO`, `gpiozero`, `VL53L1X`, `ltr559`, `hx711-multi`, `adafruit-circuitpython-charlcd`

See `environment.yml` for the full list.

## Installation

```bash
git clone https://github.com/yourusername/labsense.git
cd labsense
conda env create -f environment.yml
conda activate labsense
```

## Configuration

Create a `.env` file at the repository root:

```env
# EmonCMS API key (electricity/water consumption data)
EMONCMS_API_KEY=your_emoncms_api_key

# Logging level
LOG_LEVEL=INFO
```

SQL Server, ChemInventory, and MQTT settings are configured in `Labsense_SQL/.env`:

```env
# ChemInventory API token
CHEMINVENTORY_CONNECTION_STRING=your_cheminventory_api_token

# Toggle SQL Server inserts for ChemInventory data (True/False)
CHEMINVENTORY_INSERT_TO_SQL=True

# MQTT broker address (used by subscriber_sqlserver.py)
MQTT_SERVER=your_mqtt_broker_ip

# SQL Server connection
SQL_SERVER=your_sql_server_instance
SQL_DATABASE=labsense
SQL_TRUSTED_CONNECTION=yes
SQL_ENCRYPTION=Optional
```

Sensor scripts on the Raspberry Pi use a separate `.env` in their own directory (e.g. `Labsense_Sensors/.env`) for I2C addresses, sensor thresholds, and retry settings.

## Running the Dashboards

Generate all HTML dashboards and the landing page:

```bash
python Labsense_SQL/ChemInventory_dashboard.py
python Labsense_SQL/consumption_dashboard.py
python Labsense_SQL/Fumehood_dashboard.py
python Labsense_SQL/water_dashboard.py
python Waste/processWasteMaster.py
python create_main_dashboard.py
```

Output is written to `plots/`. Open `plots/summary_dashboard.html` in a browser.

## Running Tests

```bash
pytest
```
