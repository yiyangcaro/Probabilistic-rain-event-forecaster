## Star Schema Data Dictionary

All files live under `data/star/<run-date>/` and are in CSV format.

### fact_forecast_hourly.csv

One row per forecast hour.

Columns:
- `timestamp_utc`: ISO timestamp in UTC.
- `precip_prob`: precipitation probability (scale based on Open-Meteo, typically 0-100).
- `precip_mm`: precipitation amount in millimeters.
- `temp_c`: temperature in Celsius.
- `wind_kph`: wind speed in km/h.
- `location_id`: location identifier (joins to `dim_location`).
- `date_id`: YYYY-MM-DD (joins to `dim_date`).

### fact_forecast_daily.csv

Daily aggregates derived from hourly data.

Columns:
- `date`: YYYY-MM-DD (joins to `dim_date`).
- `precip_mm_total`: sum of hourly precipitation (mm).
- `precip_prob_max`: max hourly precipitation probability.
- `temp_c_mean`: mean hourly temperature.
- `wind_kph_mean`: mean hourly wind speed.

### dim_date.csv

Date dimension.

Columns:
- `date_id`: YYYY-MM-DD (primary key).
- `date`: YYYY-MM-DD.
- `year`: year number.
- `month`: month number (1-12).
- `day`: day of month (1-31).
- `day_of_week`: Monday=0, Sunday=6.

### dim_location.csv

Location dimension.

Columns:
- `location_id`: location identifier (primary key).
- `city`: city name.
- `latitude`: latitude coordinate.
- `longitude`: longitude coordinate.
- `timezone`: timezone string.
