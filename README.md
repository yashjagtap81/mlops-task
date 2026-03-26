# MLOps Signal Pipeline — T0 Technical Assessment

A minimal, production-style batch job that reads OHLCV data, computes a
rolling-mean signal, and emits structured metrics — fully Dockerized and
deterministic via seed + config.

---

## Repository layout

```
.
├── run.py          # pipeline entrypoint
├── config.yaml     # seed / window / version
├── __data.csv      # 10 000-row OHLCV dataset
├── requirements.txt
├── Dockerfile
├── metrics.json    # sample successful-run output
├── run.log         # sample log from that run
└── README.md
```

---

## Local run

**Prerequisites:** Python 3.9+ and `pip`.

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run the pipeline
python run.py \
  --input    __data.csv \
  --config   config.yaml \
  --output   metrics.json \
  --log-file run.log
```

Outputs:
- `metrics.json` — machine-readable metrics (written on both success **and** error)
- `run.log`      — full structured log

---

## Docker build & run

```bash
# Build
docker build -t mlops-task .

# Run (prints metrics JSON to stdout; exits 0 on success)
docker run --rm mlops-task
```

To retrieve the output files from the container:

```bash
docker run --rm \
  -v "$(pwd)/output:/app/output" \
  mlops-task \
  python run.py \
    --input    __data.csv \
    --config   config.yaml \
    --output   output/metrics.json \
    --log-file output/run.log
```

---

## Configuration (`config.yaml`)

| Key       | Type   | Description                                      |
|-----------|--------|--------------------------------------------------|
| `seed`    | int    | NumPy random seed — ensures deterministic runs   |
| `window`  | int    | Rolling-mean window size (rows)                  |
| `version` | string | Pipeline version tag (written to metrics output) |

---

## Signal logic

```
rolling_mean[t] = mean(close[t-window+1 … t])   # NaN for first window-1 rows
signal[t]       = 1  if close[t] > rolling_mean[t]
                = 0  otherwise
                = excluded  for the first window-1 rows (insufficient history)
```

`signal_rate = mean(signal)` over all **valid** (non-NaN) rows.

---

## Example `metrics.json` (success)

```json
{
  "version": "v1",
  "rows_processed": 10000,
  "metric": "signal_rate",
  "value": 0.4991,
  "latency_ms": 48,
  "seed": 42,
  "status": "success"
}
```

Error shape (always written, even on failure):

```json
{
  "version": "v1",
  "status": "error",
  "error_message": "Description of what went wrong"
}
```

---

## Reproducibility

Setting `numpy.random.seed(seed)` at startup guarantees identical outputs
across runs given the same `config.yaml` and `__data.csv`.  The `version`
field in `config.yaml` lets you tag and track pipeline iterations.

---

## Validation & error handling

`run.py` validates:

| Condition                        | Behaviour                                    |
|----------------------------------|----------------------------------------------|
| Config file missing              | Error metrics written, exit 1                |
| Missing required config key      | Error metrics written, exit 1                |
| Input CSV not found              | Error metrics written, exit 1                |
| Unparseable CSV                  | Error metrics written, exit 1                |
| Empty CSV                        | Error metrics written, exit 1                |
| `close` column absent            | Error metrics written, exit 1                |
| Non-numeric rows in `close`      | Warning logged, rows dropped, run continues  |
