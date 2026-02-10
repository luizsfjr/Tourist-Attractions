import os

# Environment detection
# Priority: explicit APP_ENV -> implicit GCP runtime indicators -> default local
_APP_ENV = os.getenv("APP_ENV", "").strip().lower()
_GCP_ENV_VARS = ("GOOGLE_CLOUD_PROJECT", "GCLOUD_PROJECT", "GCP_PROJECT", "K_SERVICE", "K_REVISION")
IS_GCP = _APP_ENV == "gcp" or any(os.getenv(v) for v in _GCP_ENV_VARS)
ENV = "gcp" if IS_GCP else "local"

if IS_GCP:
    RAW = "gs://gcp-datalakehouse-raw-3"
    BRONZE = "gs://gcp-datalakehouse-bronze-3"
    SILVER = "gs://gcp-datalakehouse-silver-3"
    GOLD = "gs://gcp-datalakehouse-gold-3"
    INVALID = "gs://gcp-datalakehouse-invalid-3"
else:
    # Local filesystem paths
    RAW = "data/raw"
    BRONZE = "data/processed/bronze"
    SILVER = "data/processed/silver"
    GOLD = "data/processed/gold"
    INVALID = "data/processed/invalid"
