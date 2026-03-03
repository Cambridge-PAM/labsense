# LabSENSE

A comprehensive IoT laboratory monitoring and management system integrating sensor data collection, chemical inventory tracking, waste management, and environmental monitoring for intelligent lab operations.

## Features

- **Chemical Inventory Management**: Track chemical stock levels with traffic light warnings (integrates with ChemInventory API)
- **Waste Management**: Process and track waste volumes per cost center (HP codes) with Excel integration
- **Utility Monitoring**: Track electricity and water consumption via EmonCMS integration
- **Sensor Monitoring**: Real-time data collection from multiple sensors (distance, light, proximity, strain gauges)
- **Environmental Monitoring**: Temperature, humidity, pressure, and gas monitoring
- **Data Visualization**: Generate interactive HTML dashboards (chemicals, waste, electricity, orders)
- **Data Management**: Multiple database backends (SQLite and SQL Server support)
- **Azure IoT Integration**: Stream sensor data to Azure IoT Hub
- **MQTT Communication**: Publish/subscribe messaging for distributed systems
- **Excel Integration**: Auto-generating reports and inventory tracking sheets

## Project Structure

```
labsense/
├── ChemInventory/          # Chemical inventory API integration
├── Consumables/            # Consumable tracking systems
├── Waste/                  # Waste processing and HP code aggregation
├── Balance Comms/          # Balance communication interface
├── Labsense_SQL/           # SQL Server data operations & dashboards
├── Labsense_SQLite/        # SQLite database operations
├── Labsense_Excel/         # Excel report generation & traffic light systems
├── Labsense_Sensors/       # Sensor interfacing code
├── RaspberryPi-4/          # Raspberry Pi 4 Azure IoT deployment scripts
├── RaspberryPi-Pico/       # Raspberry Pi Pico microcontroller firmware
├── plots/                  # Generated dashboard HTML and visualization PNGs
├── SQL/                    # SQL query documentation
├── tests/                  # Unit tests (pytest)
├── .vscode/                # VS Code debug configuration
└── .env                    # Environment variables (root level, see Configuration)
```

## Dependencies

This project uses Conda for environment management. Key dependencies include:

- **Data Processing**: pandas, numpy, matplotlib
- **Databases**: sqlite, pyodbc (SQL Server)
- **APIs & Networking**: requests, paho-mqtt
- **Excel Operations**: openpyxl
- **Configuration**: python-dotenv
- **Testing**: pytest
- **Hardware Interfaces**: RPi.GPIO, gpiozero, VL53L1X, ltr559, hx711-multi (for Raspberry Pi deployment)

See `environment.yml` for the complete dependency list.

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/labsense.git
cd labsense
```

2. Create the Conda environment:
```bash
conda env create -f environment.yml
conda activate labsense
```

3. Configure environment variables (see Configuration section below)

## Configuration

Environment variables are centralized in a `.env` file at the repository root. Create a `.env` file with the following variables:

```env
# Azure IoT Hub Connection String
IOTHUB_DEVICE_CONNECTION_STRING=your_azure_iothub_connection_string

# ChemInventory API Authentication Token
CHEMINVENTORY_CONNECTION_STRING=your_cheminventory_token

# EmonCMS API Key (for electricity/water consumption data)
EMONCMS_API_KEY=your_emoncms_api_key

# SQL Server Options
CHEMINVENTORY_INSERT_TO_SQL=True # Toggle ChemInventory SQL Server inserts (default: True)
LOG_LEVEL=INFO                  # Logging level (default: INFO)
```

The `.env` file is automatically loaded by all modules. Each module loads the `.env` file from the repository root, ensuring centralized configuration management.

### Fumehood sensor tuning (`Labsense_Sensors/.env`)

The fumehood script (`Labsense_Sensors/fumehood.py`) uses a local `.env` file in `Labsense_Sensors/` for sensor and MQTT settings.

Recommended starter values for improved VL53L1X stability:

```env
# Logging retention (daily rotation, keep last 7 files)
FUMEHOOD_LOG_RETENTION_DAYS=7
FUMEHOOD_LOG_ROTATE_WHEN=midnight

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

# Light warm-up discard
LIGHT_WARMUP_DISCARD_COUNT=2
LIGHT_WARMUP_DISCARD_DELAY_SECONDS=0.05

# Optional periodic proactive reinit (0 disables)
PROACTIVE_REINIT_INTERVAL_SECONDS=0
```

Notes:
- `FUMEHOOD_LOG_RETENTION_DAYS=7` keeps approximately one week of log history.
- Keep `TOF_TIMING_BUDGET_US <= TOF_INTER_MEASUREMENT_MS * 1000`.
- If you still see occasional zeros, increase `DISTANCE_SAMPLE_COUNT` to `5` before increasing reboot thresholds.
- If lights can legitimately be off/dark, keep `LIGHT_ZERO_RETRY_COUNT` low (for example `1`) to avoid masking real zero-lux conditions.
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
