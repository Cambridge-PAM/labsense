# Water Monitoring

This section documents the water-focused parts of LabSense, including data collection,
processing, and dashboard generation.

## Relevant Modules

- Labsense_Sensors/water-2taps.py: hardware-facing collection script for tap flow state.
- Labsense_SQL/water_dashboard.py: dashboard generation script for water insights.
- Labsense_SQL/subscriber_sqlserver.py: ingestion path used by utility data pipelines.

## Typical Workflow

1. Collect water-related signals from Raspberry Pi Pico W.
2. Ingest and store measurements in SQL Server.
3. Aggregate and visualize trends in the water dashboard.

## Setting up Water Sensors

### Hardware

Setting up the water flow tracker requires a turbine sensor installed in the water flow and a Raspberry Pi for control.

The test setup used **YF-S201C water sensors**, along with adapters that allow them to be fitted to commercial taps.

The setup has been successfully implemented using:

- Raspberry Pi Zero 2 W
- Raspberry Pi 4 Model B

The wires were protected using plastic heat-shrink tubing. It is recommended that this is applied before installation, once the wiring has been tested and confirmed to work.

---

### Single Tap Setup

#### Install the Water Sensor

Install the water sensor in the tap with the arrow pointing in the direction of water flow. Ensure all fittings are secure to minimise leakage.

(fig-water-tap-1-a)=
```{figure} img/tap-1-a.jpg
:alt: Water flow sensor installed on one tap.
:width: 70%

(fig-water-tap-1-b)=
```{figure} img/tap-1-b.jpg
:alt: Close-up of water flow sensor installed on one tap, showing the upwards arrow.
:width: 70%

(fig-water-tap-1-c)=
```{figure} img/tap-1-c.jpg
:alt: Water flow sensor wiring.
:width: 70%

(fig-water-tap-1-d)=
```{figure} img/tap-1-d.jpg
:alt: Water flow sensor pin placement (one tap).
:width: 70%

The sensor has three wires:

| Wire Colour | Function |
|------------|----------|
| Red | Power |
| Black | Ground |
| Yellow | Data output |

The sensor terminates in a 3-pin JST-XH female connector.

Use a suitable extension cable with:

- JST-XH male connector on the sensor end
- 2.54 mm Dupont female connectors on the Raspberry Pi end

#### Raspberry Pi Zero 2 Wiring

Connect the wires as follows:

| Sensor Wire | Raspberry Pi Connection |
|------------|------------------------|
| Red | 3.3 V (Pin 1) |
| Black | Ground (Pin 6) |
| Yellow | GPIO 4 (Pin 7) |

> The red and black wires may be connected to any suitable 3.3 V and ground pins.
>
> The yellow wire **must** be connected to GPIO 4 (physical pin 7) unless the software is modified.

---

### Two Tap Setup

First, install the first sensor as described above.

Install the second sensor in the same way and connect it as follows:

| Sensor Wire | Raspberry Pi Connection |
|------------|------------------------|
| Red | 3.3 V (Pin 17) |
| Black | Ground (Pin 9) |
| Yellow | GPIO 27 (Pin 13) |

> Any suitable 3.3 V or ground pin may be used.
>
> Ensure the software configuration matches the GPIO pin assignments used.

(fig-water-tap-2-a)=
```{figure} img/tap-2-a.jpg
:alt: Water flow sensor installed on two taps.
:width: 70%

(fig-water-tap-2-b)=
```{figure} img/tap-2-b.jpg
:alt: Water flow sensor pin placement (two taps).
:width: 70%

#### Position the Raspberry Pi

Mount the Raspberry Pi away from potential water exposure to reduce the risk of damage.

---

### Troubleshooting

#### Error

```text
libopenblas.so.0: cannot open shared object file: No such file or directory
```

**Solution**

```bash
sudo apt-get install libopenblas-dev
```

#### Error

```text
No module named RPi
```

This may occur even when the package appears to be installed.

**Solution**

```bash
pip install RPi.GPIO
```

---

### References

#### Single Tap Script

- TBC

#### Two Tap Script

- https://github.com/Cambridge-PAM/labsense/blob/main/Labsense_Sensors/water-2taps.py

## Running Water Dashboard Generation

From the repository root:

```bash
python Labsense_SQL/water_dashboard.py
```

## Operational Checks

- Verify environment variables in Labsense_SQL/.env are set correctly.
- Confirm source feeds (sensor or utility API) are accessible.
- Ensure SQL connectivity is available before running processing scripts.

## Troubleshooting

- If dashboard output is missing, validate that upstream ingestion has recent records.
- If SQL errors occur, re-check driver/server settings in environment configuration.
- If values appear flat or discontinuous, review sensor-side logging and timestamps.