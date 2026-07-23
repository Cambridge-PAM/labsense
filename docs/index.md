# LabSense Documentation

LabSense is a laboratory IoT monitoring and analytics platform for chemistry lab environments.
It collects sensor and operational data, processes domain-specific workflows, and produces dashboards
that help teams reduce waste, improve equipment usage, and track sustainability impact.

## What LabSense Covers

- Electricity and water consumption streams.
- Fumehood usage, sash position, and room presence signals.
- Chemical inventory and hazard-code related data.
- Solvent usage and waste processing workflows.

## System Overview

LabSense combines multiple data pipelines:

- Sensor devices (Raspberry Pi / Pico) publish telemetry.
- Integration scripts ingest data from external systems (for example ChemInventory).
- SQL and processing modules transform and store operational data.
- Dashboard scripts generate HTML summaries for daily decision support.

## Repository Modules

- Labsense_SQL: SQL-oriented processing, dashboard generation, and shared data utilities.
- Labsense_Sensors: sensor collection scripts and hardware-facing helpers.
- ChemInventory: API-oriented scripts for inventory and hazard workflows.
- Waste: waste allocation and hazard mapping workflows.
- tests: pytest coverage for core processing behavior.

## Documentation Map

- Start with [Getting Started](getting_started.md) to set up and run core workflows.
- See [LabSense Server](labsense_server.md) for server installation, MQTT (Mosquitto), and SQL Server setup.
- See [LabSense Sensors](labsense_sensors.md) for sensor installation and Raspberry Pi setup.
- See [Water Monitoring](water.md) for details of water-focused scripts and outputs.

!!! note
    This project is under active development, and documentation will continue to expand.