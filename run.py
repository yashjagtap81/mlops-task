"""
MLOps Batch Job — rolling-mean signal pipeline.
Usage:
    python run.py --input data.csv --config config.yaml \
                  --output metrics.json --log-file run.log
"""

import argparse
import json
import logging
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import yaml


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MLOps signal pipeline")
    parser.add_argument("--input",    required=True, help="Path to OHLCV CSV")
    parser.add_argument("--config",   required=True, help="Path to YAML config")
    parser.add_argument("--output",   required=True, help="Path for metrics JSON")
    parser.add_argument("--log-file", required=True, help="Path for log file")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def setup_logging(log_file: str) -> logging.Logger:
    logger = logging.getLogger("mlops_pipeline")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    # File handler — full detail
    fh = logging.FileHandler(log_file, mode="w")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    # Console handler — INFO+
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


# ---------------------------------------------------------------------------
# Config loading & validation
# ---------------------------------------------------------------------------

REQUIRED_CONFIG_KEYS = {"seed", "window", "version"}


def load_config(config_path: str, logger: logging.Logger) -> dict:
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(path, "r") as f:
        cfg = yaml.safe_load(f)

    if not isinstance(cfg, dict):
        raise ValueError("Config YAML must be a mapping (key: value pairs)")

    missing = REQUIRED_CONFIG_KEYS - cfg.keys()
    if missing:
        raise ValueError(f"Config missing required keys: {sorted(missing)}")

    if not isinstance(cfg["seed"], int):
        raise ValueError(f"'seed' must be an integer, got {type(cfg['seed']).__name__}")
    if not isinstance(cfg["window"], int) or cfg["window"] < 1:
        raise ValueError(f"'window' must be a positive integer, got {cfg['window']!r}")
    if not isinstance(cfg["version"], str) or not cfg["version"]:
        raise ValueError(f"'version' must be a non-empty string, got {cfg['version']!r}")

    logger.info(
        "Config loaded — version=%s  seed=%d  window=%d",
        cfg["version"], cfg["seed"], cfg["window"],
    )
    return cfg


# ---------------------------------------------------------------------------
# Dataset loading & validation
# ---------------------------------------------------------------------------

def load_dataset(input_path: str, logger: logging.Logger) -> pd.DataFrame:
    path = Path(input_path)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    try:
        df = pd.read_csv(path)
    except Exception as exc:
        raise ValueError(f"Could not parse CSV: {exc}") from exc

    if df.empty:
        raise ValueError("Input CSV is empty")

    if "close" not in df.columns:
        raise ValueError(
            f"Required column 'close' not found. Columns present: {list(df.columns)}"
        )

    # Coerce close to numeric; flag non-parseable rows
    original_len = len(df)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    bad_rows = df["close"].isna().sum()
    if bad_rows == original_len:
        raise ValueError("Column 'close' contains no valid numeric values")
    if bad_rows:
        logger.warning(
            "Dropping %d row(s) with non-numeric 'close' values", bad_rows
        )
        df = df.dropna(subset=["close"]).reset_index(drop=True)

    logger.info("Dataset loaded — %d rows, %d columns", len(df), len(df.columns))
    logger.debug("Columns: %s", list(df.columns))
    return df


# ---------------------------------------------------------------------------
# Processing
# ---------------------------------------------------------------------------

def compute_rolling_mean(
    df: pd.DataFrame, window: int, logger: logging.Logger
) -> pd.Series:
    """
    Rolling mean over 'close'.

    The first (window-1) rows yield NaN; those rows are excluded from
    signal computation (not filled), which is the cleaner choice for a
    trading signal — we never fabricate a signal when we lack sufficient
    history.
    """
    rolling_mean = df["close"].rolling(window=window, min_periods=window).mean()
    valid = rolling_mean.notna().sum()
    logger.info(
        "Rolling mean computed — window=%d, valid rows=%d, NaN rows=%d",
        window, valid, len(df) - valid,
    )
    return rolling_mean


def compute_signal(
    df: pd.DataFrame, rolling_mean: pd.Series, logger: logging.Logger
) -> pd.Series:
    """
    signal = 1 if close > rolling_mean else 0.
    Rows where rolling_mean is NaN are excluded (signal stays NaN there).
    """
    signal = pd.Series(np.nan, index=df.index, dtype=float)
    valid_mask = rolling_mean.notna()
    signal[valid_mask] = (df.loc[valid_mask, "close"] > rolling_mean[valid_mask]).astype(int)
    ones = int(signal.sum())
    valid_count = int(valid_mask.sum())
    logger.info(
        "Signal generated — 1s=%d, 0s=%d, excluded (NaN)=%d",
        ones, valid_count - ones, len(df) - valid_count,
    )
    return signal


# ---------------------------------------------------------------------------
# Metrics output
# ---------------------------------------------------------------------------

def write_metrics(output_path: str, payload: dict, logger: logging.Logger) -> None:
    with open(output_path, "w") as f:
        json.dump(payload, f, indent=2)
    logger.debug("Metrics written to %s", output_path)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def main() -> int:
    args = parse_args()

    # Logging must be set up before anything else so errors are captured too
    logger = setup_logging(args.log_file)

    start_ts = time.time()
    logger.info("=" * 60)
    logger.info("Job started")
    logger.info("  input  : %s", args.input)
    logger.info("  config : %s", args.config)
    logger.info("  output : %s", args.output)
    logger.info("  log    : %s", args.log_file)
    logger.info("=" * 60)

    # We don't know the version until config is loaded; default for error paths
    version = "unknown"

    try:
        # 1. Config
        cfg = load_config(args.config, logger)
        version = cfg["version"]

        # 2. Seed
        np.random.seed(cfg["seed"])
        logger.info("NumPy random seed set to %d", cfg["seed"])

        # 3. Dataset
        df = load_dataset(args.input, logger)

        # 4. Rolling mean
        rolling_mean = compute_rolling_mean(df, cfg["window"], logger)

        # 5. Signal
        signal = compute_signal(df, rolling_mean, logger)

        # 6. Metrics
        valid_signal = signal.dropna()
        rows_processed = len(df)
        signal_rate = float(valid_signal.mean())
        latency_ms = int((time.time() - start_ts) * 1000)

        metrics = {
            "version": version,
            "rows_processed": rows_processed,
            "metric": "signal_rate",
            "value": round(signal_rate, 4),
            "latency_ms": latency_ms,
            "seed": cfg["seed"],
            "status": "success",
        }

        write_metrics(args.output, metrics, logger)

        logger.info("-" * 60)
        logger.info("Metrics summary:")
        for k, v in metrics.items():
            logger.info("  %-18s = %s", k, v)
        logger.info("-" * 60)
        logger.info("Job finished successfully  (%.1f ms)", latency_ms)

        print(json.dumps(metrics, indent=2))
        return 0

    except Exception as exc:  # noqa: BLE001
        logger.error("Pipeline failed: %s", exc, exc_info=True)

        error_metrics = {
            "version": version,
            "status": "error",
            "error_message": str(exc),
        }
        try:
            write_metrics(args.output, error_metrics, logger)
        except Exception as write_exc:
            logger.error("Could not write error metrics: %s", write_exc)

        print(json.dumps(error_metrics, indent=2), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
