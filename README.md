# LabSense

[![Documentation Status](https://readthedocs.org/projects/labsense/badge/?version=latest)](https://labsense.readthedocs.io/en/latest/)

LabSense is a laboratory IoT monitoring and analytics platform for chemistry lab environments. It ingests live sensor streams, processes ChemInventory and waste data, and generates HTML dashboards for operations and reporting.

## Architecture

```
[Raspberry Pi sensors]    -> MQTT/serial -> ingestion scripts         -> SQL Server (labsense)
[ChemInventory API]       -> HTTP        -> Labsense_SQL + Balance    -> SQL Server
[Waste Master workbook]   -> file        -> Waste processing           -> dashboard outputs
[Dashboard generators]    -> SQL/files   -> plots/*.html + plots/*.png/pdf
```

## Repository Layout

| Path | Purpose |
|---|---|
| `Labsense_SQL/` | SQL Server ingestion and analytics dashboards (`ChemInventory`, `consumption`, `fumehood`, `water`, `sen66`) |
| `Labsense_Sensors/` | Raspberry Pi sensor publishers and helpers (fumehood, SEN66, water flow, device email/IP utilities) |
| `Waste/` | Waste normalization and hazard-code allocation workflow plus waste dashboards |
| `Balance/` | Balance integration scripts (serial weighing and ChemInventory update workflow) |
| `Consumables/` | Consumables and strain-calibration scripts |
| `Labsense_Excel/` | Legacy Excel-driven operational scripts |
| `Analytics/` | Analysis notebooks and case study notebooks/scripts |
| `docs/` | MkDocs documentation source |
| `tests/` | Project-level pytest suite |
| `create_main_dashboard.py` | Generates `plots/index.html` linking discovered `*dashboard.html` files |

## Environment and Dependencies

The project is managed with Conda via `environment.yml`.

Core dependencies currently include:

- Data and analytics: `pandas`, `numpy`, `scipy`, `scikit-learn`, `matplotlib`
- Database/connectivity: `pyodbc`, `sqlite`
- Integrations: `paho-mqtt`, `requests`, `openpyxl`, `python-dotenv`
- Testing: `pytest`

Install:

```bash
git clone https://github.com/Cambridge-PAM/labsense.git
cd labsense
conda env create -f environment.yml
conda activate labsense
```

## Configuration

Most SQL/processing scripts load variables from `Labsense_SQL/.env`.

Typical variables:

```env
EMONCMS_API_KEY=your_emoncms_api_key
EMONCMS_BASE_URL=https://your_emoncms_host

CHEMINVENTORY_CONNECTION_STRING=your_cheminventory_api_token
CHEMINVENTORY_INSERT_TO_SQL=True

MQTT_SERVER=your_mqtt_broker_ip

SQL_SERVER=your_sql_server_instance
SQL_DATABASE=labsense
SQL_TRUSTED_CONNECTION=yes
SQL_ENCRYPTION=Optional

PLOTS_DIR=plots
LOG_LEVEL=INFO
```

Raspberry Pi sensor scripts use `Labsense_Sensors/.env` for hardware-specific settings.

## Generating Dashboards

Run the dashboard generators:

```bash
python Labsense_SQL/ChemInventory_dashboard.py
python Labsense_SQL/consumption_dashboard.py
python Labsense_SQL/Fumehood_dashboard.py
python Labsense_SQL/water_dashboard.py
python Labsense_SQL/sen66_dashboard.py
python Waste/processWasteMaster.py
python create_main_dashboard.py
```

Outputs are written to `plots/`. Open `plots/index.html` to access the dashboard landing page.

## Testing

```bash
pytest
```

## Documentation

Build docs locally:

```bash
pip install -r docs/requirements.txt
mkdocs serve
```

Then open the local MkDocs URL (usually `http://127.0.0.1:8000`).
