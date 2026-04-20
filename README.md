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
# SQL Server connection string (Windows Trusted Auth)
CHEMINVENTORY_CONNECTION_STRING=DRIVER={ODBC Driver 18 for SQL Server};SERVER=MSM-FPM-70203\LABSENSE;DATABASE=labsense;Trusted_Connection=yes;

# ChemInventory API token (see ChemInventory/authtoken.py)
CHEMINVENTORY_API_TOKEN=your_token_here

# Toggle SQL Server inserts for ChemInventory data (True/False)
CHEMINVENTORY_INSERT_TO_SQL=True

# Azure IoT Hub device connection string
IOTHUB_DEVICE_CONNECTION_STRING=your_azure_iothub_connection_string

# EmonCMS API key (electricity/water data)
EMONCMS_API_KEY=your_emoncms_api_key

# Logging level
LOG_LEVEL=INFO
```

Sensor scripts on the Raspberry Pi use a separate `.env` in their own directory (e.g. `Labsense_Sensors/.env`) for MQTT broker address, I2C addresses, sensor thresholds, and retry settings.

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

# Optional explicit timing (set both to non-zero to enable set_timing + start_ranging(0))
TOF_TIMING_BUDGET_US=50000
TOF_INTER_MEASUREMENT_MS=70

# Initialization/recovery stabilization
SENSOR_STABILIZE_DELAY_SECONDS=1.0
DISTANCE_WARMUP_DISCARD_COUNT=2
DISTANCE_WARMUP_DISCARD_DELAY_SECONDS=0.05

# Per-cycle filtering and transient zero mitigation
DISTANCE_SAMPLE_COUNT=3
DISTANCE_SAMPLE_DELAY_SECONDS=0.03
DISTANCE_ZERO_RETRY_COUNT=1
DISTANCE_ZERO_RETRY_DELAY_SECONDS=0.05
LIGHT_SAMPLE_COUNT=3
LIGHT_SAMPLE_DELAY_SECONDS=0.03
LIGHT_ZERO_RETRY_COUNT=1
LIGHT_ZERO_RETRY_DELAY_SECONDS=0.05
LIGHT_I2C_ERROR_RETRY_COUNT=2
LIGHT_I2C_ERROR_RETRY_DELAY_SECONDS=0.1

# Light warm-up discard
LIGHT_WARMUP_DISCARD_COUNT=2
LIGHT_WARMUP_DISCARD_DELAY_SECONDS=0.05
LIGHT_READ_ERROR_REINIT_THRESHOLD=3

# Optional periodic proactive reinit (0 disables)
PROACTIVE_REINIT_INTERVAL_SECONDS=0
```

Notes:
- `FUMEHOOD_LOG_RETENTION_DAYS=7` keeps approximately one week of log history.
- Keep `TOF_TIMING_BUDGET_US <= TOF_INTER_MEASUREMENT_MS * 1000`.
- If you still see occasional zeros, increase `DISTANCE_SAMPLE_COUNT` to `5` before increasing reboot thresholds.
- If lights can legitimately be off/dark, keep `LIGHT_ZERO_RETRY_COUNT` low (for example `1`) to avoid masking real zero-lux conditions.
- For intermittent `[Errno 121] Remote I/O error`, tune `LIGHT_I2C_ERROR_RETRY_COUNT` first, then `LIGHT_READ_ERROR_REINIT_THRESHOLD`.
- Start with `PROACTIVE_REINIT_INTERVAL_SECONDS=0`; only enable periodic reinit if long-running lockups persist.

## Usage

Different modules can be deployed based on use case:

### Dashboard Generation & Data Processing
- **Main Dashboard**: `create_main_dashboard.py` - Generates index page linking to all dashboards
- **Electricity Dashboard**: `Labsense_SQL/daily_consumption_sqlserver.py` - Process daily consumption data from EmonCMS
  - Supports date range arguments: `--start-date YYYY-MM-DD --end-date YYYY-MM-DD`
- **Chemical Inventory Dashboard**: `Labsense_SQL/ChemInventory_sqlserver.py` - Sync ChemInventory data to SQL Server
- **Waste Processing**: `Waste/processWasteMaster.py` - Aggregate waste volumes by HP code

### Desktop/Server Scripts
- `Labsense_Excel/ChemInventory-Traffic-Light-System.py` - Generate chemical inventory Excel report with traffic light colors
- `Labsense_SQL/`: SQL Server data operations and dashboard generation
- `ChemInventory/`: Direct ChemInventory API integration for inventory management

### Raspberry Pi 4
- Azure IoT streaming (`RaspberryPi-4/Azure-Stream-*.py`) - Stream various sensor data to Azure
- Sensor data publishing (`publisher.py`)
- MQTT subscription and logging (`subscriber*.py`)

### Raspberry Pi Pico
- Lightweight microcontroller firmware
- WiFi connectivity
- Onboard sensor reading

## Data Visualization

Generated dashboards are stored in the `plots/` directory:
- `cheminventory_dashboard.html` - Chemical inventory status with trend charts
- `electricity_dashboard.html` - Electricity consumption analysis
- `Waste_dashboard.html` - Waste volume trends by category and quarter
- `index.html` - Main landing page with links to all dashboards

## Testing

The project uses pytest for unit testing. Run tests with:

```bash
pytest tests/
pytest tests/ -v                    # Verbose output
pytest tests/test_cheminventory_size_parsing.py -v  # Specific test file
```

Test coverage includes:
- ChemInventory size parsing
- SQL insert toggle configuration
- Main processing functions
- Environment variable validation

## Development

### VS Code Debugging

VS Code launch configurations are included in `.vscode/launch.json`:

- **Python: Current File** - Debug the currently open file
- **Python: Daily Consumption** - Debug daily consumption script with date arguments
- **Python: Labsense_SQL ChemInventory** - Debug ChemInventory sync
- **Python: Create Main Dashboard** - Debug dashboard generation
- **Python: Pytest** - Run tests in debug mode

Access these via the Debug view (Ctrl+Shift+D) or Run menu.

## Hardware Requirements (for Raspberry Pi deployment)

- **Sensors**: VL53L1X (distance), LTR559 (light/proximity), BME680 (environmental), strain gauges
- **Microcontrollers**: Raspberry Pi 4, Raspberry Pi Pico
- **Interfaces**: I2C, GPIO, Serial
- **Databases**: SQLite (local) or SQL Server (centralized)

## License

See LICENSE file for details.
