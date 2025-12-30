# LabSENSE

A comprehensive IoT laboratory monitoring and management system integrating sensor data collection, chemical inventory tracking, and environmental monitoring for intelligent lab operations.

## Features

- **Sensor Monitoring**: Real-time data collection from multiple sensors (distance, light, proximity, strain gauges)
- **Chemical Inventory Management**: Track chemical stock levels with traffic light warnings
- **Water & Fume Hood Monitoring**: Real-time tracking of water consumption and fume hood operations
- **Environmental Monitoring**: Temperature, humidity, pressure, and gas monitoring
- **Data Management**: Multiple database backends (SQLite and SQL Server support)
- **Azure IoT Integration**: Stream sensor data to Azure IoT Hub
- **MQTT Communication**: Publish/subscribe messaging for distributed systems
- **Excel Integration**: Auto-generating reports and inventory tracking sheets

## Project Structure

```
labsense/
├── Balance Comms/          # Balance communication interface
├── ChemInventory/          # Chemical inventory tracking
├── Consumables/            # Consumable tracking systems
├── Labsense_Excel/         # Excel report generation
├── Labsense_Sensors/       # Sensor interfacing code
├── Labsense_SQL/           # SQL Server integrations
├── Labsense_SQLite/        # SQLite database operations
├── RaspberryPi-4/          # Raspberry Pi 4 deployment scripts
├── RaspberryPi-Pico/       # Raspberry Pi Pico firmware
└── SQL/                    # SQL query documentation
```

## Dependencies

This project uses Conda for environment management. Key dependencies include:

- **Data Processing**: pandas, numpy
- **Databases**: sqlite, pyodbc (SQL Server)
- **IoT/Networking**: paho-mqtt, azure-iot-device, requests
- **Hardware Interfaces**: RPi.GPIO, gpiozero, VL53L1X, ltr559, hx711-multi
- **Excel Operations**: openpyxl
- **Configuration**: python-dotenv
- **Scheduling**: schedule

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

## Configuration

Environment variables should be configured in a `.env` file. Required variables include:
- Database connection strings (SQL Server)
- MQTT broker details
- Azure IoT Hub connection strings
- API authentication tokens

## Usage

Different modules can be deployed based on use case:

### Desktop/Server Scripts
- `Labsense_Excel/`: Generate and update inventory reports
- `Labsense_SQL/`: SQL Server data operations
- `ChemInventory/`: Chemical inventory management

### Raspberry Pi 4
- Azure IoT streaming (`RaspberryPi-4/Azure-Stream-*.py`)
- Sensor data publishing (`publisher.py`)
- MQTT subscription and logging (`subscriber*.py`)

### Raspberry Pi Pico
- Lightweight microcontroller firmware
- WiFi connectivity and MQTT
- Onboard sensor reading

## Hardware Requirements

- **Sensors**: VL53L1X (distance), LTR559 (light/proximity), BME680 (environmental)
- **Microcontrollers**: Raspberry Pi 4, Raspberry Pi Pico
- **Interfaces**: I2C, GPIO, Serial
- **Databases**: SQLite (local) or SQL Server (centralized)

## License

See LICENSE file for details.
