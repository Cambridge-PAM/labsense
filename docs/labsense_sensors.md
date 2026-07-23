# LabSense Sensors

This page describes how to prepare a Raspberry Pi 4 or Raspberry Pi Zero 2 W
for LabSense sensor workloads and how to run sensor scripts as a managed
service.

The process below is suitable for devices used for water monitoring and other
Pi-hosted LabSense sensor scripts.

## 1. Supported Raspberry Pi Devices

The following devices have been used successfully for LabSense sensor work:

- Raspberry Pi 4 Model B
- Raspberry Pi Zero 2 W

For most sensor deployments, use Raspberry Pi OS Lite unless a desktop
environment is required for debugging.

## 2. Prepare the Raspberry Pi

### 2.1 Flash Raspberry Pi OS

1. Install Raspberry Pi Imager on your workstation.
2. Flash the microSD card with Raspberry Pi OS Lite.
3. In the Imager advanced options, set:
    - hostname
    - username and password
    - Wi-Fi SSID and password if the device will use wireless networking
    - SSH enabled
    - locale and time zone

!!! note
    A Raspberry Pi Zero 2 W is usually deployed over Wi-Fi, while a Raspberry Pi 4
    can use either Ethernet or Wi-Fi.

### 2.2 First Boot and System Update

Connect to the Pi over SSH and update the base system:

```bash
sudo apt update
sudo apt full-upgrade -y
sudo reboot
```

After reboot, reconnect and install core packages:

```bash
sudo apt install -y git python3-venv python3-pip
```

### 2.3 Enable Required Interfaces

Enable any hardware interfaces needed by the specific sensor scripts:

```bash
sudo raspi-config
```

Typical interfaces to enable:

- I2C for supported sensor boards
- SSH for remote administration
- Serial or SPI only if required by the attached hardware

## 3. Create the LabSense Runtime Account

Create a dedicated service account if the device is being prepared from scratch:

```bash
sudo adduser labsense
sudo usermod -aG sudo labsense
```

Switch to that account before continuing with the repository and virtual
environment setup:

```bash
su - labsense
```

## 4. Clone the Repository

Create the repository working copy in the Labsense home directory:

```bash
git clone https://github.com/Cambridge-PAM/labsense.git
cd labsense
git config pull.rebase false
git config pull.ff only
git pull
```

The examples in this page assume the repository path is:

```text
/home/labsense/labsense
```

## 5. Create the Python Environment

Create a dedicated virtual environment for the Raspberry Pi:

```bash
python3 -m venv /home/labsense/iot
source /home/labsense/iot/bin/activate
pip install --upgrade pip
pip install -r /home/labsense/labsense/docs/requirements.txt
```

!!! note
    Some Raspberry Pi sensor packages may need additional system libraries or GPIO
    access depending on the hardware attached to the device.

## 6. Create the Repo Update Script

Create a helper script so the device can refresh the repository before starting
the sensor workload:

```bash
nano /home/labsense/update_repo.sh
```

Use this content:

```bash
#!/bin/bash

cd /home/labsense/labsense
git pull --ff-only
```

Make it executable:

```bash
chmod +x /home/labsense/update_repo.sh
```

## 7. Create the Startup Script

Create the script that activates the Python environment, updates the repository,
and starts the LabSense sensor processes:

```bash
nano /home/labsense/startup.sh
```

Use this content:

```bash
#!/bin/bash

source /home/labsense/iot/bin/activate

/home/labsense/update_repo.sh

python /home/labsense/labsense/Labsense_Sensors/email_ip.py
python /home/labsense/labsense/Labsense_Sensors/water-2taps.py
```

Make it executable:

```bash
chmod +x /home/labsense/startup.sh
```

!!! note
    In this example, `email_ip.py` runs first and `water-2taps.py` then starts the
    long-running sensor process. If your deployment uses a different sensor script,
    replace `water-2taps.py` with the correct entry point.

## 8. Create the systemd Service

Create the LabSense service definition:

```bash
sudo nano /etc/systemd/system/labsense.service
```

Use this content:

```ini
[Unit]
Description=Labsense
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=labsense
WorkingDirectory=/home/labsense
ExecStart=/home/labsense/startup.sh
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Reload systemd and enable the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable labsense.service
```

Start the service:

```bash
sudo systemctl start labsense.service
```

Check the service status:

```bash
sudo systemctl status labsense.service
```

View recent logs if startup fails:

```bash
journalctl -u labsense.service -n 100 --no-pager
```

## 9. Operational Checks

Before handing the device over for routine use, verify the following:

- The Pi is reachable on the network.
- The correct LabSense sensor script starts automatically after reboot.
- GPIO and sensor interfaces are available to the `labsense` user.
- Any required `.env` file or configuration file is present.
- Data is reaching the downstream LabSense services or MQTT broker.

## 10. Updating a Running Sensor Device

When the service is already installed, refresh the code and restart the service:

```bash
sudo systemctl stop labsense.service
/home/labsense/update_repo.sh
sudo systemctl start labsense.service
```

## 11. Troubleshooting

### Service fails immediately

Check:

- `/home/labsense/startup.sh` exists and is executable
- `/home/labsense/iot/bin/activate` exists
- the referenced Python scripts exist at the expected paths

### Python package import errors

Activate the environment manually and run the target script directly:

```bash
source /home/labsense/iot/bin/activate
python /home/labsense/labsense/Labsense_Sensors/water-2taps.py
```

### Service starts before the network is ready

The service file already declares `network-online.target`, but you should still
confirm that the device has a reliable network configuration and working DNS.