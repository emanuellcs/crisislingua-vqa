import logging
from typing import List, Dict, Any
from .adaptive_client import AdaptiveDataClient

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(name)s - %(message)s"
)
logger = logging.getLogger("NoiseFilter")


class NoiseFilter:
    """
    Dedicated module to interface with the Adaptive Data platform for
    filtering out irrelevant chatter and noise from raw multimodal streams.
    """

    def __init__(self, client: AdaptiveDataClient = None):
        # Allow dependency injection of the client for testing, or instantiate a new one
        self.client = client or AdaptiveDataClient()

    def process_batch(self, batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Sends a batch of records to the platform to strip irrelevant noise.
        """
        if not batch:
            return []

        logger.info(f"Filtering noise from a batch of {len(batch)} records...")

        try:
            filtered_batch = self.client.reshape_batch(batch, operation="filter_noise")
            logger.info(
                f"Noise filtering complete. Retained {len(filtered_batch)} actionable records."
            )
            return filtered_batch
        except Exception as e:
            logger.error(f"Failed to filter noise for batch: {e}")
            raise
