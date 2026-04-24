import os
import sys
import time
import random
import logging
from typing import List, Dict, Any
from adaption import Adaption, APIStatusError
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
    Follows the Ingest -> Adapt -> Export workflow with hyper-conservative network policies.
    """

    def __init__(self):
        # Aligning with official SDK defaults and environment variable naming
        self.api_key = os.getenv("ADAPTION_API_KEY") or os.getenv(
            "ADAPTIVE_DATA_API_KEY"
        )
        self.base_url = os.getenv("ADAPTION_BASE_URL") or os.getenv(
            "ADAPTIVE_DATA_ENDPOINT"
        )

        if not self.api_key:
            raise ValueError("ADAPTION_API_KEY is missing from environment variables.")

        self.consecutive_failures = 0
        self.user_agent = "CrisisLingua-Research-Bot/1.0 (Uncharted Data Challenge; Research purpose; limit 20req/min)"

        # Initialize client with manual retry management for strict policy compliance
        self.client = Adaption(
            api_key=self.api_key,
            base_url=self.base_url,
            max_retries=0,  # Manual handling to satisfy circuit breaker requirement
            default_headers={"User-Agent": self.user_agent},
        )

    def _execute_with_policy(self, func, *args, **kwargs):
        """Wraps SDK calls with the required circuit breaker and rate limiting policies."""
        while True:
            try:
                # Mandatory jitter before every SDK interaction
                time.sleep(random.uniform(3.0, 8.0))

                return func(*args, **kwargs)

            except APIStatusError as e:
                if e.status_code in (403, 429):
                    self.consecutive_failures += 1
                    if self.consecutive_failures >= 3:
                        logger.critical(
                            "Circuit Breaker Tripped (Adaption SDK): Halting to prevent IP ban"
                        )
                        sys.exit(1)

                    # Extract Retry-After if available in the response headers
                    retry_after = e.response.headers.get("Retry-After")
                    if retry_after and retry_after.isdigit():
                        sleep_time = int(retry_after)
                        logger.warning(
                            f"Adaption API {e.status_code}. Sleeping for {sleep_time}s."
                        )
                        time.sleep(sleep_time)
                    else:
                        sleep_time = random.uniform(5.0, 15.0)
                        logger.warning(
                            f"Adaption API {e.status_code}. Sleeping for {sleep_time:.2f}s before retry."
                        )
                        time.sleep(sleep_time)
                    continue

                # For other errors, reset failure count and raise
                self.consecutive_failures = 0
                raise
            except Exception:
                self.consecutive_failures = 0
                raise

    def reshape_batch(
        self, batch: List[Dict[str, Any]], operation: str
    ) -> List[Dict[str, Any]]:
        """
        Sends a batch of raw records to the Adaptive Data platform using the SDK.
        Maps the requested workflow (ingest, adapt, export) to the SDK's datasets resource.
        """
        logger.info(
            f"Processing batch of {len(batch)} records for operation: '{operation}'"
        )

        try:
            # 1. Ingest: Create a temporary dataset from the batch
            dataset = self._execute_with_policy(
                self.client.datasets.create,
                records=batch,
                metadata={"source": "CrisisLingua-VQA", "operation": operation},
            )
            dataset_id = dataset.id

            # 2. Adapt: Run the specific adaptation recipe/operation
            job = self._execute_with_policy(
                self.client.datasets.run,
                dataset_id=dataset_id,
                recipe=operation,
                config={
                    "strict_schema_enforcement": True,
                    "multilingual_projection": True,
                },
            )

            # 3. Export: Download the reshaped records
            reshaped_records = self._execute_with_policy(
                self.client.datasets.download, dataset_id=dataset_id, format="jsonl"
            )

            # Reset failure count on full workflow success
            self.consecutive_failures = 0
            return reshaped_records

        except Exception as e:
            logger.error(f"Adaptive Data SDK operation failed: {e}")
            raise AdaptiveDataError(f"Failed to reshape batch via SDK: {e}")
