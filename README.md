## Probabilistic Rain Event Forecaster (Montreal, 2018–2024)

### Goal

Predict the probability that it will rain **tomorrow** in Montreal, using historical daily weather data.

### Data

- Source: [Open-Meteo Historical Weather API](https://open-meteo.com/)
- Location: Montreal (45.5088 N, -73.5878 W)
- Period: 2018-01-01 to 2024-12-31
- Daily features used:
  - rain_sum, snowfall_sum, precipitation_sum
  - temperature_2m_max, temperature_2m_min, temperature_2m_mean
  - wind_speed_10m_max, wind_gusts_10m_max

Target label:

- `rain_tomorrow = 1` if next day `rain_sum > 0.5 mm`, else `0`.

### Method

- Train / test split by time (e.g. 80% train, 20% test).
- Model: **Logistic Regression** (scikit-learn)
  - Trained to output \(P(\text{rain tomorrow} = 1 \mid \text{features})\).
- Because the data are **imbalanced** (about ~33% rainy tomorrows), I:
  - Compared against a **baseline** that always predicts "no rain".
  - Used **ROC AUC** and **F1-score** instead of accuracy alone.
  - Searched over thresholds \(t \in [0.05, 0.95]\) to choose a decision threshold.

### Results (test set)

- Baseline (always "no rain"):
  - Accuracy: **0.664**

- Logistic regression, default threshold 0.5:
  - Accuracy: ≈ **0.586**
  - ROC AUC: **0.664**

- Logistic regression, threshold chosen to maximize **F1**:
  - Threshold: **0.35**
  - Accuracy: ≈ **0.525**
  - F1-score (rain class): ≈ **0.559**
  - ROC AUC: **0.664**

I deliberately choose the **F1-optimal threshold** for the final classifier, because missing rainy days is more costly than an occasional false alarm.


### How to run

Prerequisites: Python 3 and project dependencies.

Install dependencies with a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate      # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
```

Run the pipeline for a specific date:

A) CLI:
```bash
PYTHONPATH=src python3 -m rain_alert.cli run --run-date YYYY-MM-DD
```

B) Direct function call:
```bash
PYTHONPATH=src python3 - <<'PY'
from rain_alert.config import Settings
from rain_alert.pipeline import run_pipeline

run_pipeline("YYYY-MM-DD", Settings())
PY
```

Outputs and locations:
- data/raw/forecast_raw_<run-date>.json
- data/processed/forecast_hourly_<run-date>.csv
- data/processed/dim_date_<run-date>.csv
- data/processed/dim_location_<run-date>.csv
- data/processed/forecast_summary_<run-date>.csv
- data/star/<run-date>/fact_forecast_hourly.csv
- data/star/<run-date>/fact_forecast_daily.csv
- data/star/<run-date>/dim_date.csv
- data/star/<run-date>/dim_location.csv
- reports/validation/validation_<run-date>.json
- reports/exceptions/exceptions_<run-date>.csv
- reports/runs/run_<run-date>.json

Pass vs fail:
- pass: no ERROR severity validation checks failed
- fail: at least one ERROR severity validation check failed; details are in the validation JSON and exceptions CSV

Run report (no BI required):
```bash
python3 scripts/make_run_report.py YYYY-MM-DD
```
Outputs are written to `reports/run_reports/run_report_<run-date>.md` and `reports/run_reports/run_report_<run-date>.csv`.
#### Example: predicting rain probability

After running the notebook top-to-bottom (so that the model clf, BEST_T,
FEATURES and predict_rain_prob are defined), you can use the helper
function in a new cell:

```python
today = {
    "rain_sum": 1.0,
    "snowfall_sum": 0.0,
    "precipitation_sum": 1.0,    
    "temperature_2m_max": 3.0,
    "temperature_2m_min": -2.0,
    "temperature_2m_mean": 0.5,
    "wind_speed_10m_max": 20.0,
    "wind_gusts_10m_max": 35.0,
}
p, label = predict_rain_prob(today)
print(f"Probability of rain tomorrow: {p:.2%}")
print("Prediction:", "rain" if label == 1 else "no rain")
```

Example output:
```text
Probability of rain tomorrow: 41.08%
Prediction: no rain
```

### Power BI

Generate star schema outputs for a specific run date:

```bash
PYTHONPATH=src python3 -m rain_alert.cli run --run-date 2026-01-24
```

Star schema files are written to:

```
data/star/2026-01-24/
  fact_forecast_hourly.csv
  fact_forecast_daily.csv
  dim_date.csv
  dim_location.csv
```

Data quality exceptions for a Power BI "Data Quality" page:

```
reports/exceptions/exceptions_2026-01-24.csv
```

For full Power BI setup steps and relationships, see `bi/README.md`.
