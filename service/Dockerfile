# service/Dockerfile — Global Phase 5 inference service
FROM python:3.10-slim AS base

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir --default-timeout=1000 -r requirements.txt

COPY app.py /app/app.py

# POC/easy deployment: copy models into the image.
# Place these 4 files under service/models/ before docker build:
#   artifacts_phase4.pkl
#   lgbm_quantile_p50_final.pkl
#   lgbm_quantile_p90_final.pkl
#   lgbm_next_type.pkl
COPY models /models

ENV MODEL_DIR=/models \
    P50_MODEL_PATH=/models/lgbm_quantile_p50_final.pkl \
    P90_MODEL_PATH=/models/lgbm_quantile_p90_final.pkl \
    TYPE_MODEL_PATH=/models/lgbm_next_type.pkl \
    ARTIFACTS_PATH=/models/artifacts_phase4.pkl \
    SERVICE_VERSION=phase5_global_v1 \
    TYPE_CONF_THRESHOLD=0.6 \
    MIN_MARGIN_SECS=5.0

EXPOSE 8080
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
