## Power BI Setup (Star Schema)

This report uses the star schema outputs produced in `data/star/<run-date>/`.

### 1) Generate a run

From repo root:

```bash
PYTHONPATH=src python3 -m rain_alert.cli run --run-date 2026-01-24
```

This creates:
```
data/star/2026-01-24/
  fact_forecast_hourly.csv
  fact_forecast_daily.csv
  dim_date.csv
  dim_location.csv
```

### 2) Load data into Power BI

1. Open Power BI Desktop.
2. Home -> Get data -> Text/CSV.
3. Load the four CSVs from `data/star/<run-date>/`.

### 3) Set relationships

In Model view, create these relationships:

- `fact_forecast_hourly[date_id]` -> `dim_date[date_id]` (Many to One, single direction)
- `fact_forecast_hourly[location_id]` -> `dim_location[location_id]`
- `fact_forecast_daily[date_id]` -> `dim_date[date_id]` (Many to One)
- `fact_forecast_daily[location_id]` -> `dim_location[location_id]`

### 4) Recommended pages

Suggested report pages:

- Forecast Overview: hourly precipitation probability and totals by date.
- Forward-looking Risk Flags: high-probability windows (use `precip_prob` thresholds).
- Data Quality: import exceptions report from `reports/exceptions/exceptions_<run-date>.csv`.

### 5) Refresh workflow

For a new run date:

1. Run the pipeline for the new date.
2. In Power BI, refresh the data source to the new folder.
