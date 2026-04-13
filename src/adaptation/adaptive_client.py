import os
import logging
from typing import List, Dict, Any
from adaption import Adaption
from tenacity import retry, wait_exponential, stop_after_attempt
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("AdaptiveClient")

class AdaptiveDataError(Exception):
    """Custom exception for Adaptive Data platform failures."""
    pass

class AdaptiveDataClient:
    """
    Production SDK wrapper for the Adaptive Data platform.
    Executes dynamic reshaping, intent extraction, and schema mapping via the official SDK.
    Follows the Ingest -> Adapt -> Export workflow.
    """

    def __init__(self):
        # Aligning with official SDK defaults and environment variable naming
        self.api_key = os.getenv("ADAPTION_API_KEY") or os.getenv("ADAPTIVE_DATA_API_KEY")
        self.base_url = os.getenv("ADAPTION_BASE_URL") or os.getenv("ADAPTIVE_DATA_ENDPOINT")

        if not self.api_key:
            raise ValueError("ADAPTION_API_KEY is missing from environment variables.")

        # Initialize client with base_url as per SDK documentation
        self.client = Adaption(
            api_key=self.api_key, 
            base_url=self.base_url
        )

    @retry(
        wait=wait_exponential(multiplier=1.5, min=2, max=15),
        stop=stop_after_attempt(5)
    )
    def reshape_batch(self, batch: List[Dict[str, Any]], operation: str) -> List[Dict[str, Any]]:
        """
        Sends a batch of raw records to the Adaptive Data platform using the SDK.
        Maps the requested workflow (ingest, adapt, export) to the SDK's datasets resource.
        """
        logger.info(f"Processing batch of {len(batch)} records for operation: '{operation}'")

        try:
            # 1. Ingest: Create a temporary dataset from the batch
            # Note: SDK usually supports direct record ingestion or file upload
            dataset = self.client.datasets.create(
                records=batch,
                metadata={"source": "CrisisLingua-VQA", "operation": operation}
            )
            dataset_id = dataset.id

            # 2. Adapt: Run the specific adaptation recipe/operation
            # Using datasets.run as shown in the official documentation
            job = self.client.datasets.run(
                dataset_id=dataset_id,
                recipe=operation, # Mapping the operation string to the recipe parameter
                config={
                    "strict_schema_enforcement": True,
                    "multilingual_projection": True
                }
            )

            # 3. Export: Download the reshaped records
            # Using datasets.download to retrieve the final JSONL payload
            reshaped_records = self.client.datasets.download(
                dataset_id=dataset_id,
                format="jsonl"
            )

            return reshaped_records

        except Exception as e:
            logger.error(f"Adaptive Data SDK operation failed: {e}")
            raise AdaptiveDataError(f"Failed to reshape batch via SDK: {e}")