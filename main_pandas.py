import logging
from transform import run_transform
from ingestion import run_ingestion


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("Starting step: transform.run_ingestion")
    run_ingestion()
    logger.info("Completed step: transform.run_ingestion")
    logger.info("Starting step: transform.run_transform")
    run_transform()
    logger.info("Completed step: transform.run_transform")
    logger.info("Pipeline finished successfully")
