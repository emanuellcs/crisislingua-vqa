import os
import sys
import time
import random
import logging
import json
import tempfile
from typing import List, Dict, Any
from adaption import Adaption, APIStatusError
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

    @staticmethod
    def _record_prompt(record: Dict[str, Any]) -> str:
        """Builds a stable prompt column for the Adaption augmentation API."""
        for key in ("prompt", "text", "title", "content", "description", "message"):
            value = record.get(key)
            if value:
                return str(value)
        return json.dumps(record, ensure_ascii=False, sort_keys=True)

    def _write_batch_jsonl(self, batch: List[Dict[str, Any]], operation: str) -> str:
        """Serializes an in-memory batch to the SDK's supported JSONL file upload path."""
        tmp = tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".jsonl",
            prefix=f"crisislingua_{operation}_",
            encoding="utf-8",
            delete=False,
        )
        with tmp:
            for record in batch:
                sdk_record = dict(record)
                sdk_record["__adaptive_operation"] = operation
                sdk_record["__adaptive_prompt"] = self._record_prompt(record)
                sdk_record["__source_record"] = json.dumps(
                    record, ensure_ascii=False, sort_keys=True
                )
                tmp.write(json.dumps(sdk_record, ensure_ascii=False) + "\n")
        return tmp.name

    @staticmethod
    def _parse_jsonl_payload(payload: Any) -> List[Dict[str, Any]]:
        """Normalizes SDK download payloads into a list of dictionaries."""
        if isinstance(payload, list):
            return payload
        if not isinstance(payload, str):
            raise TypeError(f"Unsupported download payload type: {type(payload)!r}")

        records = []
        for line in payload.splitlines():
            if line.strip():
                records.append(json.loads(line))
        return records

    @staticmethod
    def _strip_internal_columns(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        internal_keys = {
            "__adaptive_operation",
            "__adaptive_prompt",
            "__source_record",
        }
        return [
            {key: value for key, value in record.items() if key not in internal_keys}
            for record in records
        ]

    @staticmethod
    def _operation_blueprint(operation: str) -> str:
        blueprints = {
            "filter_noise": (
                "Keep crisis-relevant humanitarian records and remove irrelevant "
                "social chatter. Preserve useful source fields."
            ),
            "extract_intent": (
                "Normalize multilingual or code-switched crisis reports into clear "
                "humanitarian intents while preserving source context."
            ),
            "map_fema_esf": (
                "Map crisis reports to FEMA ESF and MIRA-style humanitarian response "
                "categories. Preserve the original source context."
            ),
        }
        return blueprints.get(
            operation,
            f"Apply the CrisisLingua-VQA adaptation operation '{operation}' to each record.",
        )

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
            # 1. Ingest: the SDK accepts datasets through file sources, not records=.
            upload_path = self._write_batch_jsonl(batch, operation)
            dataset = self._execute_with_policy(
                self.client.datasets.upload_file,
                upload_path,
                name=f"CrisisLingua-VQA-{operation}-{int(time.time())}",
            )
            dataset_id = dataset.dataset_id

            self._execute_with_policy(
                self.client.datasets.wait_for_completion,
                dataset_id,
                timeout=1800.0,
            )

            # 2. Adapt: run the augmentation pipeline with SDK-supported params.
            self._execute_with_policy(
                self.client.datasets.run,
                dataset_id,
                column_mapping={
                    "prompt": "__adaptive_prompt",
                    "context": ["__source_record", "__adaptive_operation"],
                },
                recipe_specification={
                    "version": "1",
                    "recipes": {
                        "deduplication": operation == "filter_noise",
                        "prompt_rephrase": operation
                        in {"extract_intent", "map_fema_esf"},
                    },
                },
                brand_controls={
                    "length": "concise",
                    "hallucination_mitigation": True,
                    "blueprint": self._operation_blueprint(operation),
                },
            )

            self._execute_with_policy(
                self.client.datasets.wait_for_completion,
                dataset_id,
                timeout=1800.0,
            )

            # 3. Export: Download the reshaped records.
            payload = self._execute_with_policy(
                self.client.datasets.download, dataset_id, file_format="jsonl"
            )

            # Reset failure count on full workflow success
            self.consecutive_failures = 0
            return self._strip_internal_columns(self._parse_jsonl_payload(payload))

        except Exception as e:
            logger.error(f"Adaptive Data SDK operation failed: {e}")
            raise AdaptiveDataError(f"Failed to reshape batch via SDK: {e}")
        finally:
            if "upload_path" in locals():
                try:
                    os.unlink(upload_path)
                except OSError:
                    logger.warning(
                        f"Could not remove temporary upload file: {upload_path}"
                    )
