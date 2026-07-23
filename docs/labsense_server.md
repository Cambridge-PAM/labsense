# LabSense Server

This page describes how to set up and operate the LabSense server host.
The server is responsible for receiving MQTT telemetry, storing data in SQL Server,
and running processing/dashboard scripts.

## 1. Server Prerequisites

- Operating system: [PLACEHOLDER: Windows/Linux version used in production]
- CPU/RAM baseline: [PLACEHOLDER: minimum and recommended specs]
- Network:
  - Static IP or reserved DHCP lease
  - Access to MQTT clients (sensor devices)
  - Access to SQL Server instance
  - Access to outbound APIs (if ChemInventory or external services are used)

## 2. Install Conda and Python

LabSense server setup should use Conda so Python and package versions match the project environment definition.

### 2.1 Choose Python Version

Use the Python version defined by the project environment file.
Current baseline in environment.yml: Python 3.9.

### 2.2 Install Conda (Windows)

1. Install Miniconda (recommended) or Anaconda.
2. Open Anaconda Prompt or PowerShell configured for Conda.
3. Verify Conda is available:

```powershell
conda --version
```

[PLACEHOLDER: approved installer source/version policy]

### 2.3 Install Conda (Linux)

Install Miniconda, then initialize shell support and reopen your shell.

```bash
conda --version
```

[PLACEHOLDER: Linux installation commands for your target distro and security policy]

## 3. Create the LabSense Conda Environment

Create and activate the project environment:

```bash
conda env create -f environment.yml
conda activate labsense
```

Verify Python version inside the environment:

```bash
python --version
```

If you are updating an existing environment:

```bash
conda env update -f environment.yml --prune
```

!!! note
  Use the `labsense` Conda environment for all server scripts, scheduled tasks, and service wrappers.

## 4. Configure Environment Variables

Create and maintain `.env` files required by server scripts.

Primary path: `Labsense_SQL/.env`

Typical keys:

- EMONCMS_API_KEY
- EMONCMS_BASE_URL
- LOG_LEVEL
- CHEMINVENTORY_CONNECTION_STRING
- CHEMINVENTORY_INSERT_TO_SQL
- MQTT_SERVER
- SQL_SERVER
- SQL_DATABASE
- SQL_TRUSTED_CONNECTION
- SQL_ENCRYPTION

[PLACEHOLDER: full validated key list for production]

## 5. Set Up MQTT Broker (Mosquitto)

### 5.1 Install Mosquitto

Windows (example):

1. Install Eclipse Mosquitto from the official installer.
2. Install as a Windows service.
3. Confirm service is running.

Linux (Ubuntu/Debian):

```bash
sudo apt update
sudo apt install -y mosquitto mosquitto-clients
sudo systemctl enable mosquitto
sudo systemctl start mosquitto
sudo systemctl status mosquitto
```

### 5.2 Configure Windows Firewall

- Open Windows Firewall with Advanced Security.
- Create a New Inbound Rule:
  a. Click on Inbound Rules in the left pane.
  b. Click on New Rule... in the right pane.
  c. Choose Port and click Next.
  d. Select TCP and specify port 1883, then click Next.
  e. Select Allow the connection, then click Next.
  f. Choose the profiles this rule applies to (Domain, Private, Public), then click Next.
  g. Name the rule (for example MQTT Port 1883), then click Finish.

### 5.3 Basic Broker Configuration

Edit Mosquitto config file:

- Linux common path: `/etc/mosquitto/mosquitto.conf`
- Windows path: [PLACEHOLDER: local Mosquitto config path]

Example baseline (adjust for your security policy):

```conf
listener 1883
allow_anonymous false
password_file /etc/mosquitto/passwd
persistence true
persistence_location /var/lib/mosquitto/
log_dest file /var/log/mosquitto/mosquitto.log
```

Create/update credentials:

```bash
sudo mosquitto_passwd -c /etc/mosquitto/passwd labsense
sudo systemctl restart mosquitto
```

### 5.4 Verify Broker Connectivity

Publisher test:

```bash
mosquitto_pub -h <broker-host> -u <user> -P <password> -t labsense/test -m "hello"
```

Subscriber test:

```bash
mosquitto_sub -h <broker-host> -u <user> -P <password> -t labsense/test
```

### 5.5 MQTT Message Format

LabSense publishers should send JSON payloads in this structure:

```json
[
  {
    "labId": 1,
    "sublabId": 3,
    "sensorReadings": {
      "water": 0.12
    },
    "measureTimestamp": "2024-01-11 13:48:00"
  }
]
```

!!! note
  JSON does not allow trailing commas. Keep the payload as shown above to avoid parser errors in subscribers.

## 6. Set Up SQL Server

### 6.1 Install and Provision SQL Server

- SQL Server host: [PLACEHOLDER: hostname or instance naming standard]
- SQL version: SQL Server 2022 Express
- Database name: `labsense`

Use SQL Server 2022 Express to mitigate deployment risk and licensing overhead.
Download it from:

- [SQL Server downloads](https://www.microsoft.com/en-gb/sql-server/sql-server-downloads)

Important constraints:

- SQL Server 2022 Express is free up to a 10 GB database size limit.
- It is intended for local or private-network deployment and should not be exposed directly for public online access.

Install the following companion tools:

- SQL Server Management Studio (SSMS) to manage the SQL Server instance.
- Microsoft ODBC Driver 18 for SQL Server so Python scripts can communicate with the database.

### 6.2 Configure Connectivity

- Open SQL Server port (default TCP 1433) as required.
- Enable SQL authentication or integrated auth based on deployment policy.

### 6.3 Create Database Objects

[PLACEHOLDER: schema migration or table creation workflow]

[PLACEHOLDER: link to SQL bootstrap script once available]

### 6.4 Validate Connection from Python

```python
import pyodbc

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=<server>;DATABASE=<db>;Trusted_Connection=yes;"
    "Encrypt=Optional;"
)
print("Connected")
conn.close()
```

## 7. Server-Side Scripts and Responsibilities

The following scripts are typically run on the server host.

### 7.1 Ingestion and Subscriber

- `Labsense_SQL/subscriber_sqlserver.py`
  - Subscribes to MQTT topics and inserts data into SQL Server.

[PLACEHOLDER: system service/scheduler definition for continuous run]

### 7.2 Processing and Dashboard Generation

- `Labsense_SQL/ChemInventory_dashboard.py`
- `Labsense_SQL/consumption_dashboard.py`
- `Labsense_SQL/Fumehood_dashboard.py`
- `Labsense_SQL/water_dashboard.py`
- `Waste/processWasteMaster.py`
- `create_main_dashboard.py`

Typical manual run sequence:

```bash
python Labsense_SQL/ChemInventory_dashboard.py
python Labsense_SQL/consumption_dashboard.py
python Labsense_SQL/Fumehood_dashboard.py
python Labsense_SQL/water_dashboard.py
python Waste/processWasteMaster.py
python create_main_dashboard.py
```

### 7.3 Recommended Automation

- Continuous scripts (subscriber): run as a service.
- Batch scripts (dashboards/waste): run via scheduler.

Windows options:

- Task Scheduler
- NSSM/Windows Service wrapper

Linux options:

- systemd services/timers
- cron

[PLACEHOLDER: canonical schedule and retention policy]

## 8. Health Checks and Monitoring

- Verify subscriber process is running.
- Verify MQTT topic ingest rate is non-zero.
- Verify latest SQL timestamps are current.
- Verify dashboard output files are regenerated on schedule.
- Review logs for auth, network, or DB connectivity failures.

[PLACEHOLDER: alerting and observability stack]

## 9. Troubleshooting Quick Checks

- MQTT connection failures:
  - Check broker host/port/credentials and firewall rules.
- SQL connection failures:
  - Check server name, auth mode, ODBC driver availability, and encryption setting.
- No new dashboard data:
  - Confirm subscriber is receiving messages and SQL tables are updating.
- Environment mismatch:
  - Verify active environment and installed package versions.
