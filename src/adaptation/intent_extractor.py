import logging
from typing import List, Dict, Any
from .adaptive_client import AdaptiveDataClient

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(name)s - %(message)s"
)
logger = logging.getLogger("IntentExtractor")


class IntentExtractor:
    """
    Dedicated module for extracting formalized intents from code-switched
    and highly informal localized text via the Adaptive Data platform.
    """

    def __init__(self, client: AdaptiveDataClient = None):
        self.client = client or AdaptiveDataClient()

    def process_batch(self, batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Processes a batch to infer intent from chaotic inputs (e.g., Tagalog mixed with English)
        and reshapes them into standardized intent arrays.
        """
        if not batch:
            return []

        logger.info(f"Extracting intents from a batch of {len(batch)} records...")

        try:
            intent_batch = self.client.reshape_batch(batch, operation="extract_intent")
            logger.info(f"Intent extraction complete for {len(intent_batch)} records.")
            return intent_batch
        except Exception as e:
            logger.error(f"Failed to extract intents for batch: {e}")
            raise
