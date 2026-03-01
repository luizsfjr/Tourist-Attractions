# ingestion.py
import os
from google.cloud import storage
import project_config


def getenv_required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Missing required env var: {name}")
    return value


def upload_file_to_gcs(
    project_id: str,
    bucket_name: str,
    source_file_path: str,
    destination_blob_path: str,
) -> None:
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_path)
    blob.upload_from_filename(source_file_path)
    print(
        f"Uploaded '{source_file_path}' to "
        f"'gs://{bucket_name}/{destination_blob_path}'"
    )


def run_ingestion():
    if(project_config.IS_GCP):
        # Required parameters
        PROJECT_ID = getenv_required("PROJECT_ID")
        RAW_BUCKET = getenv_required("RAW_BUCKET")  # example: gcp-datalakehouse-raw-3
        SOURCE_FILE_PATH = os.getenv("SOURCE_FILE_PATH", "data/raw/Attraction_Belem.csv")
        DESTINATION_BLOB_PATH = os.getenv(
            "DESTINATION_BLOB_PATH",
            "Attraction_Belem.csv",  # object path inside bucket
        )

        upload_file_to_gcs(
            project_id=PROJECT_ID,
            bucket_name=RAW_BUCKET,
            source_file_path=SOURCE_FILE_PATH,
            destination_blob_path=DESTINATION_BLOB_PATH,
        )
    else:
        print("Local execution!")
if __name__ == "__main__":
    run_ingestion()
