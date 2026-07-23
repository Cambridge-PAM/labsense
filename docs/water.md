# Water Monitoring

This section documents the water-focused parts of LabSense, including data collection,
processing, and dashboard generation.

## Relevant Modules

- Labsense_Sensors/water-2taps.py: hardware-facing collection script for tap flow state.
- Labsense_SQL/water_dashboard.py: dashboard generation script for water insights.
- Labsense_SQL/emoncms_data_sqlserver.py: ingestion path used by utility data pipelines.

## Typical Workflow

1. Collect water-related signals from edge devices.
2. Ingest and store measurements in SQL Server.
3. Aggregate and visualize trends in the water dashboard.

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