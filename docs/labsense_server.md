# LabSense Server

This page describes how to set up and operate the LabSense server host.
The server is responsible for receiving MQTT telemetry, storing data in SQL Server,
and running processing/dashboard scripts.

## 1. Server Prerequisites

- Operating system: tested with Windows 11 Pro 25H2.
- CPU/RAM baseline: tested with Intel i9-14900 2.00 GHz processor with 64 GB of RAM.
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

### 2.3 Install Conda (Linux)

Install Miniconda, then initialize shell support and reopen your shell.

```bash
conda --version
```

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
    - Click on Inbound Rules in the left pane.
    - Click on New Rule... in the right pane.
    - Choose Port and click Next.
    - Select TCP and specify port 1883, then click Next.
    - Select Allow the connection, then click Next.
    - Choose the profiles this rule applies to (Domain, Private, Public), then click Next.
    - Name the rule (for example MQTT Port 1883), then click Finish.

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
    "ipAddress": "10.247.1.1",
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

- SQL Server host: `labsense`
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

The subscriber can also be run as a scheduled start-up task.

This is an exemplar script to run the subscriber.

```bash
mosquitto
C:\Users\fpm-admin\.conda\envs\labsense\python.exe C:\Users\fpm-admin\src\labsense\Labsense_SQL\subscriber_sqlserver.py
```

Dashboard creation scripts are usually run on a daily cadence.

This is an exemplar script to run dashboard creation to a network folder.

```bash
@echo off
setlocal enabledelayedexpansion

echo [DIAGNOSTIC] Script started
echo [DIAGNOSTIC] Setting variables...

REM ============================================================
REM  CONFIGURATION
REM ============================================================
set LOGFILE=C:\Labsense\Logs\labsense_task.log
set PYTHON_LOG=C:\Labsense\Logs\python_output.log
set CONDA_PATH=C:\ProgramData\miniconda3
set ENV_NAME=labsense
set ENV_PATH=C:\Users\fpm-admin\.conda\envs\labsense

echo [DIAGNOSTIC] Variables set
echo [DIAGNOSTIC] LOGFILE: %LOGFILE%
echo [DIAGNOSTIC] CONDA_PATH: %CONDA_PATH%

REM Network share credentials
set SHARE=\\folder\
set DRIVE=Z:
set SHARE_USER=USERNAME
set SHARE_PASS=PASSWORD

echo [DIAGNOSTIC] Network variables set

REM Python scripts
set SCRIPT1=C:\Users\fpm-admin\src\labsense\Labsense_SQL\ChemInventory_sqlserver.py
set SCRIPT2=C:\Users\fpm-admin\src\labsense\Labsense_SQL\daily_consumption_sqlserver.py
set SCRIPT3=C:\Users\fpm-admin\src\labsense\Labsense_SQL\granular_consumption_sqlserver.py
set SCRIPT4=C:\Users\fpm-admin\src\labsense\Waste\processWasteMaster.py
set SCRIPT5=C:\Users\fpm-admin\src\labsense\Labsense_SQL\ChemInventory_dashboard.py
set SCRIPT6=C:\Users\fpm-admin\src\labsense\Labsense_SQL\consumption_dashboard.py
set SCRIPT7=C:\Users\fpm-admin\src\labsense\Labsense_SQL\Fumehood_dashboard.py
set SCRIPT8=C:\Users\fpm-admin\src\labsense\Labsense_SQL\water_dashboard.py
set SCRIPT9=C:\Users\fpm-admin\src\labsense\Labsense_SQL\sen66_dashboard.py

echo [DIAGNOSTIC] Script paths set
echo [DIAGNOSTIC] Creating logs directory...

REM ============================================================
REM  SETUP - Create logs directory
REM ============================================================
if not exist "C:\Labsense\Logs" (
    echo [DIAGNOSTIC] Directory does not exist, creating...
    mkdir "C:\Labsense\Logs"
    if !ERRORLEVEL! neq 0 (
        echo ERROR: Failed to create logs directory
        pause
        exit /b 1
    )
    echo [DIAGNOSTIC] Directory created successfully
) else (
    echo [DIAGNOSTIC] Directory already exists
)

echo [DIAGNOSTIC] About to define log function...

REM ============================================================
REM  LOG ROTATION - KEEP LAST 7 DAYS
REM ============================================================
echo [DIAGNOSTIC] Cleaning up logs older than 7 days...
forfiles /p "C:\Labsense\Logs" /m "*.log" /d -7 /c "cmd /c del /q @path" >nul 2>&1
echo [DIAGNOSTIC] Log cleanup complete

REM ============================================================
REM  LOGGING FUNCTION
REM ============================================================
goto :main

:log
echo [%date% %time%] %~1 >> "!LOGFILE!"
echo [%date% %time%] %~1
goto :eof

:main
echo [DIAGNOSTIC] Log function defined
echo [DIAGNOSTIC] Starting execution
call :log "=========================================================="
call :log "TASK STARTED"
call :log "Machine: %COMPUTERNAME%"
call :log "User: %USERNAME%"
call :log "Working directory: %CD%"
call :log "=========================================================="

REM ============================================================
REM  CHECK CONDA
REM ============================================================
call :log "Checking Conda installation at: !CONDA_PATH!"
if not exist "!CONDA_PATH!" (
    call :log "ERROR: Conda not found at !CONDA_PATH!"
    call :log "Please install Miniconda3"
    goto error
)
call :log "Conda found"

REM ============================================================
REM  CLEAN UP EXISTING DRIVE MAPPING
REM ============================================================
call :log "Cleaning up existing network drive mapping..."
net use !DRIVE! /delete /yes >nul 2>&1

REM ============================================================
REM  MAP NETWORK DRIVE
REM ============================================================
call :log "Mapping network drive !DRIVE! to !SHARE!"
echo [DIAGNOSTIC] DRIVE=!DRIVE!
echo [DIAGNOSTIC] SHARE=!SHARE!
echo [DIAGNOSTIC] SHARE_USER=!SHARE_USER!

REM Convert to regular variables before disabling delayed expansion
setlocal disabledelayedexpansion
set "DRIVE=%DRIVE%"
set "SHARE=%SHARE%"
set "SHARE_USER=%SHARE_USER%"
set "SHARE_PASS=%SHARE_PASS%"

echo [DIAGNOSTIC] About to run: net use %DRIVE% "%SHARE%" /user:%SHARE_USER%

net use %DRIVE% "%SHARE%" /user:%SHARE_USER% "%SHARE_PASS%" /persistent:no
set MAPERROR=%ERRORLEVEL%
endlocal & set MAPERROR=%MAPERROR%
setlocal enabledelayedexpansion

if !MAPERROR! neq 0 (
    call :log "ERROR: Failed to map network drive. Error code: !MAPERROR!"
    call :log "Listing current network connections:"
    net use >> "!LOGFILE!"
    goto error
)
call :log "Network drive mapped successfully"

REM ============================================================
REM  PREPARE CONDA RUN (avoid activate in Task Scheduler)
REM ============================================================
set "CONDA_BAT=!CONDA_PATH!\condabin\conda.bat"
call :log "Preparing Conda run for environment: !ENV_NAME!"
call :log "Using conda.bat: !CONDA_BAT!"
call :log "Using env path: !ENV_PATH!"

if not exist "!CONDA_BAT!" (
    call :log "ERROR: conda.bat not found at !CONDA_BAT!"
    goto error
)

call :log "Collecting Conda diagnostics"
call "!CONDA_BAT!" info >> "!PYTHON_LOG!" 2>&1
call "!CONDA_BAT!" env list >> "!PYTHON_LOG!" 2>&1
call :log "Listing envs directory: !CONDA_PATH!\envs"
dir "!CONDA_PATH!\envs" >> "!PYTHON_LOG!" 2>&1

call "!CONDA_BAT!" run -p "!ENV_PATH!" python --version >> "!PYTHON_LOG!" 2>&1
if !ERRORLEVEL! neq 0 (
    call :log "ERROR: Conda run failed for environment !ENV_NAME!"
    goto error
)
call :log "Conda environment ready"

REM ============================================================
REM  RUN PYTHON SCRIPTS
REM ============================================================
call :log "Starting Python script execution"

set SCRIPT_NUM=0
for %%S in (
    "!SCRIPT1!"
    "!SCRIPT2!"
    "!SCRIPT3!"
    "!SCRIPT4!"
    "!SCRIPT5!"
    "!SCRIPT6!"
    "!SCRIPT7!"
    "!SCRIPT8!"
    "!SCRIPT9!"
) do (
    set /a SCRIPT_NUM+=1
    call :log "---"
    call :log "Running script !SCRIPT_NUM!: %%~nxS"
    
    call "!CONDA_BAT!" run -p "!ENV_PATH!" python "%%~S" >> "!PYTHON_LOG!" 2>&1
    set EXIT_CODE=!ERRORLEVEL!
    
    if !EXIT_CODE! neq 0 (
        call :log "ERROR: Script failed with exit code !EXIT_CODE!: %%~S"
        goto error
    )
    call :log "Script completed successfully"
)

REM ============================================================
REM  CLEANUP AND SUCCESS
REM ============================================================
call :log "All scripts completed successfully"
call :log "Cleaning up network drive mapping..."
net use !DRIVE! /delete /yes >nul 2>&1

call :log "=========================================================="
call :log "TASK COMPLETED SUCCESSFULLY"
call :log "=========================================================="

echo.
echo All scripts completed successfully!
echo.
pause
endlocal
exit /b 0

REM ============================================================
REM  ERROR HANDLER
REM ============================================================
:error
call :log "=========================================================="
call :log "TASK FAILED"
call :log "=========================================================="
call :log "Cleaning up network drive mapping..."
net use !DRIVE! /delete /yes >nul 2>&1

echo.
echo ERROR: Task failed. Check log file:
echo   %LOGFILE%
echo.
endlocal
exit /b 1
```

## 8. Health Checks and Monitoring

- Verify subscriber process is running.
- Verify MQTT topic ingest rate is non-zero.
- Verify latest SQL timestamps are current.
- Verify dashboard output files are regenerated on schedule.
- Review logs for auth, network, or DB connectivity failures.

## 9. Troubleshooting Quick Checks

- MQTT connection failures:
    - Check broker host/port/credentials and firewall rules.
- SQL connection failures:
    - Check server name, auth mode, ODBC driver availability, and encryption setting.
- No new dashboard data:
    - Confirm subscriber is receiving messages and SQL tables are updating.
- Environment mismatch:
    - Verify active environment and installed package versions.
