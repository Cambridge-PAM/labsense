# Getting Started

This guide covers the fastest path to get LabSense running locally for development,
testing, and dashboard generation.

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

Typical values include:

- EMONCMS_API_KEY
- EMONCMS_BASE_URL
- CHEMINVENTORY_CONNECTION_STRING
- MQTT_SERVER
- SQL_SERVER
- SQL_DATABASE

## Run Core Dashboards

Generate the HTML dashboards and top-level summary page:

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