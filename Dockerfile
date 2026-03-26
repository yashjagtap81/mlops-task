FROM python:3.9-slim

# ── System deps (none beyond pip needed for this slim stack) ────────────────
WORKDIR /app

# ── Python deps (cached layer) ──────────────────────────────────────────────
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Application code + data ─────────────────────────────────────────────────
COPY run.py       .
COPY config.yaml  .
COPY __data.csv   .

# ── Default command ─────────────────────────────────────────────────────────
# All paths are relative to WORKDIR; no hard-coded absolute paths.
CMD ["python", "run.py", \
     "--input",    "__data.csv", \
     "--config",   "config.yaml", \
     "--output",   "metrics.json", \
     "--log-file", "run.log"]
