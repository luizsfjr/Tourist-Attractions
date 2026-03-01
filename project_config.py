import os

# Environment detection
# Priority: explicit APP_ENV -> implicit GCP runtime indicators -> default local
_APP_ENV = os.getenv("APP_ENV", "").strip().lower()
_GCP_ENV_VARS = ("GOOGLE_CLOUD_PROJECT", "GCLOUD_PROJECT", "GCP_PROJECT", "K_SERVICE", "K_REVISION")
IS_GCP = _APP_ENV == "gcp" or any(os.getenv(v) for v in _GCP_ENV_VARS)
ENV = "gcp" if IS_GCP else "local"

if IS_GCP:
    RAW = "gs://gcp-lc-datalakehouse-raw"
    BRONZE = "gs://gcp-lc-datalakehouse-bronze"
    SILVER = "gs://gcp-lc-datalakehouse-silver"
    GOLD = "gs://gcp-lc-datalakehouse-gold"
    INVALID = "gs://gcp-datalakehouse-invalid"
else:
    # Local filesystem paths
    RAW = "data/raw"
    BRONZE = "data/processed/bronze"
    SILVER = "data/processed/silver"
    GOLD = "data/processed/gold"
    INVALID = "data/processed/invalid"
