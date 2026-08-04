# Getting Started

This guide covers the fastest path to get LabSense running locally for development,
testing, and dashboard generation.

## Read These Setup Guides First (Required)

Before running dashboard scripts, complete these pages in order:

1. [LabSense Server Setup](labsense_server.md)
	- SQL Server setup and connectivity
	- MQTT broker setup
	- required `Labsense_SQL/.env` variables
2. [LabSense Sensors Setup](labsense_sensors.md)
	- Raspberry Pi sensor host setup
	- sensor-side `.env` configuration
3. [ChemInventory Setup and Pipeline](cheminventory.md)
	- ChemInventory API key/token requirements
	- SQL sync flow used by ChemInventory dashboard
4. [Water Monitoring Setup](water.md)
	- water sensor wiring, collection script behavior, and dashboard dependencies

The `Run Core Dashboards` commands below assume these setup steps are already complete.

## Prerequisites

- Git
- Conda (recommended, based on project environment file)
- Access to required external systems if running full pipelines:
	- SQL Server instance
	- MQTT broker
	- ChemInventory API token

## Clone and Create Environment

```bash
git clone https://github.com/yourusername/labsense.git
cd labsense
conda env create -f environment.yml
conda activate labsense
```

## Configure Environment Variables

Create or update environment files used by scripts:

- Labsense_SQL/.env for SQL and processing scripts
- Labsense_Sensors/.env for hardware sensor scripts

Use these docs to confirm values before running pipelines:

- [LabSense Server Setup](labsense_server.md) for SQL Server, MQTT, and server-side env keys.
- [ChemInventory Setup and Pipeline](cheminventory.md) for `CHEMINVENTORY_CONNECTION_STRING` and ChemInventory sync requirements.
- [Water Monitoring Setup](water.md) for water sensor pipeline configuration and MQTT payload path.

Typical values include:

- EMONCMS_API_KEY
- EMONCMS_BASE_URL
- CHEMINVENTORY_CONNECTION_STRING
- MQTT_SERVER
- SQL_SERVER
- SQL_DATABASE

## Run Core Dashboards

Generate the HTML dashboards and top-level summary page:

Prerequisite checks:

- SQL Server is reachable and populated with recent data.
- MQTT ingestion path is running for sensor-backed dashboards.
- ChemInventory token is configured and ChemInventory SQL sync has been run.
- Sensor-side setup has been completed for feeds used by your dashboards.

```bash
python Labsense_SQL/ChemInventory_dashboard.py
python Labsense_SQL/consumption_dashboard.py
python Labsense_SQL/Fumehood_dashboard.py
python Labsense_SQL/water_dashboard.py
python Waste/processWasteMaster.py
python create_main_dashboard.py
```

Generated outputs are written to the plots directory.

## Run Tests

```bash
pytest
```

## Build Documentation Locally

Install docs dependencies and build:

```bash
pip install -r docs/requirements.txt
mkdocs serve
```

The local docs site is then available at the URL shown by MkDocs (usually http://127.0.0.1:8000).