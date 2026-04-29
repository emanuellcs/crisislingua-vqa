import asyncio
import hashlib
import json
import logging
import os
import random
import sys
import tempfile
import threading
import time
from collections import deque
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlsplit, urlunsplit

import httpx
from adaption import APIStatusError, Adaption, AsyncAdaption
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("AdaptiveClient")


class AdaptiveDataError(Exception):
    """Custom exception for Adaptive Data platform failures."""


class AsyncRateLimiter:
    """Small async-compatible sliding-window limiter for Adaption API calls."""

    def __init__(self, max_calls: int, period_seconds: float = 60.0):
        self.max_calls = max(1, max_calls)
        self.period_seconds = period_seconds
        self._timestamps = deque()
        self._lock = threading.Lock()

    async def wait(self):
        while True:
            with self._lock:
                now = time.monotonic()
                while (
                    self._timestamps
                    and now - self._timestamps[0] >= self.period_seconds
                ):
                    self._timestamps.popleft()

                if len(self._timestamps) < self.max_calls:
                    self._timestamps.append(now)
                    return

                sleep_for = self.period_seconds - (now - self._timestamps[0])

            await asyncio.sleep(max(sleep_for, 0.05))


def run_coroutine_sync(coro):
    """
    Run an async workflow from a synchronous caller.

    Kaggle notebooks often already have an event loop, so a background thread is
    used in that case instead of calling asyncio.run() directly.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    result: Dict[str, Any] = {}

    def _runner():
        try:
            result["value"] = asyncio.run(coro)
        except BaseException as exc:  # pragma: no cover - re-raised in caller
            result["error"] = exc

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()
    thread.join()

    if "error" in result:
        raise result["error"]
    return result.get("value")


class AdaptiveDataClient:
    """
    Production SDK wrapper for the Adaptive Data platform.

    The fast path is asynchronous and decouples upload/run from polling/export.
    The legacy reshape_batch() API is retained for staged compatibility.
    """

    RETRYABLE_STATUS_CODES = {403, 429, 502, 503, 504}
    RATE_LIMIT_STATUS_CODES = {403, 429}
    INTERNAL_COLUMNS = {
        "__adaptive_operation",
        "__adaptive_prompt",
        "__source_record",
    }

    def __init__(self):
        self.api_key = os.getenv("ADAPTION_API_KEY") or os.getenv(
            "ADAPTIVE_DATA_API_KEY"
        )
        raw_base_url = os.getenv("ADAPTION_BASE_URL") or os.getenv(
            "ADAPTIVE_DATA_ENDPOINT"
        )
        self.base_url = self._normalize_base_url(raw_base_url)

        if not self.api_key:
            raise ValueError("ADAPTION_API_KEY is missing from environment variables.")

        self.consecutive_failures = 0
        self._failure_lock = threading.Lock()
        self.circuit_breaker_limit = int(
            os.getenv("ADAPTION_CIRCUIT_BREAKER_LIMIT", "3")
        )
        self.rate_limit_per_minute = int(
            os.getenv("ADAPTION_RATE_LIMIT_PER_MINUTE", "18")
        )
        self.min_jitter_seconds = float(
            os.getenv("ADAPTION_MIN_JITTER_SECONDS", "0.25")
        )
        self.max_jitter_seconds = float(os.getenv("ADAPTION_MAX_JITTER_SECONDS", "1.0"))
        self.ingestion_timeout_seconds = float(
            os.getenv("ADAPTION_INGESTION_TIMEOUT_SECONDS", "1800")
        )
        self.run_timeout_seconds = float(
            os.getenv("ADAPTION_RUN_TIMEOUT_SECONDS", "43200")
        )
        self.download_chunk_bytes = int(
            os.getenv("ADAPTION_DOWNLOAD_CHUNK_BYTES", str(1024 * 1024))
        )
        self.upload_chunk_bytes = int(
            os.getenv("ADAPTION_UPLOAD_CHUNK_BYTES", str(1024 * 1024))
        )
        self.user_agent = (
            "CrisisLingua-Research-Bot/1.0 "
            "(Uncharted Data Challenge; Research purpose; limit 20req/min)"
        )

        self.client = Adaption(
            api_key=self.api_key,
            base_url=self.base_url,
            max_retries=0,
            default_headers={"User-Agent": self.user_agent},
        )
        self.async_client = AsyncAdaption(
            api_key=self.api_key,
            base_url=self.base_url,
            max_retries=0,
            default_headers={"User-Agent": self.user_agent},
        )
        self._api_rate_limiter = AsyncRateLimiter(self.rate_limit_per_minute)

    @staticmethod
    def _normalize_base_url(base_url: Optional[str]) -> Optional[str]:
        """
        The generated SDK owns the /api/v1 path. Older project configs included
        /v1 or /api/v1 in ADAPTION_BASE_URL, which makes the SDK POST to
        /v1/api/v1/datasets or /api/v1/api/v1/datasets.
        """
        if not base_url or not base_url.strip():
            return None

        candidate = base_url.strip().rstrip("/")
        parsed = urlsplit(candidate)
        normalized_path = parsed.path.rstrip("/")

        if normalized_path in {"/v1", "/api/v1"}:
            normalized = urlunsplit(
                (parsed.scheme, parsed.netloc, "", parsed.query, parsed.fragment)
            ).rstrip("/")
            logger.warning(
                "Normalizing ADAPTION_BASE_URL from %s to %s because the SDK "
                "appends /api/v1 endpoint paths itself.",
                base_url,
                normalized,
            )
            return normalized

        return candidate

    def _jitter_delay(self) -> float:
        low = min(self.min_jitter_seconds, self.max_jitter_seconds)
        high = max(self.min_jitter_seconds, self.max_jitter_seconds)
        return random.uniform(low, high)

    def _increment_failures(self) -> int:
        with self._failure_lock:
            self.consecutive_failures += 1
            return self.consecutive_failures

    def _reset_failures(self):
        with self._failure_lock:
            self.consecutive_failures = 0

    @staticmethod
    def _retry_after_seconds(error: APIStatusError) -> Optional[float]:
        headers = getattr(getattr(error, "response", None), "headers", {}) or {}
        retry_after = headers.get("Retry-After")
        if retry_after and str(retry_after).isdigit():
            return float(retry_after)
        return None

    def _execute_with_policy(self, func, *args, **kwargs):
        """Sync policy wrapper retained for compatibility with older callers."""
        while True:
            try:
                time.sleep(self._jitter_delay())
                result = func(*args, **kwargs)
                self._reset_failures()
                return result
            except APIStatusError as e:
                if e.status_code not in self.RETRYABLE_STATUS_CODES:
                    self._reset_failures()
                    raise

                if e.status_code in self.RATE_LIMIT_STATUS_CODES:
                    failures = self._increment_failures()
                    if failures >= self.circuit_breaker_limit:
                        logger.critical(
                            "Circuit Breaker Tripped (Adaption SDK): "
                            "halting to prevent IP ban"
                        )
                        sys.exit(1)

                sleep_time = self._retry_after_seconds(e) or random.uniform(5.0, 15.0)
                logger.warning(
                    "Adaption API %s. Sleeping for %.2fs before retry.",
                    e.status_code,
                    sleep_time,
                )
                time.sleep(sleep_time)
            except Exception:
                self._reset_failures()
                raise

    async def _execute_async_with_policy(self, func, *args, **kwargs):
        """Async SDK call wrapper with global rate limiting and circuit breaking."""
        while True:
            try:
                await self._api_rate_limiter.wait()
                await asyncio.sleep(self._jitter_delay())
                result = await func(*args, **kwargs)
                self._reset_failures()
                return result
            except APIStatusError as e:
                if e.status_code not in self.RETRYABLE_STATUS_CODES:
                    self._reset_failures()
                    raise

                if e.status_code in self.RATE_LIMIT_STATUS_CODES:
                    failures = self._increment_failures()
                    if failures >= self.circuit_breaker_limit:
                        raise AdaptiveDataError(
                            "Circuit Breaker Tripped (Adaption SDK): "
                            "halting to prevent IP ban"
                        ) from e

                sleep_time = self._retry_after_seconds(e) or random.uniform(5.0, 15.0)
                logger.warning(
                    "Adaption API %s. Sleeping for %.2fs before retry.",
                    e.status_code,
                    sleep_time,
                )
                await asyncio.sleep(sleep_time)
            except Exception:
                self._reset_failures()
                raise

    def _wait_for_ingestion_ready(
        self,
        dataset_id: str,
        *,
        timeout: float,
        initial_interval: float = 2.0,
        max_interval: float = 30.0,
        backoff_factor: float = 2.0,
    ):
        """Sync ingestion poller retained for compatibility."""
        started_at = time.monotonic()
        interval = initial_interval
        last_status = None

        while True:
            status = self._execute_with_policy(
                self.client.datasets.get_status, dataset_id
            )
            last_status = status.status

            if status.error:
                raise AdaptiveDataError(status.error.message)

            if status.status == "failed":
                raise AdaptiveDataError(
                    f"Dataset ingestion failed for {dataset_id}: {status.error}"
                )

            if status.row_count is not None:
                logger.info(
                    "Dataset %s ingestion ready with %s rows.",
                    dataset_id,
                    status.row_count,
                )
                return status

            if time.monotonic() - started_at >= timeout:
                raise TimeoutError(
                    f"Timed out after {timeout}s waiting for dataset ingestion; "
                    f"last status: {last_status}"
                )

            time.sleep(interval)
            interval = min(interval * backoff_factor, max_interval)

    async def wait_for_ingestion_ready_async(
        self,
        dataset_id: str,
        *,
        timeout: Optional[float] = None,
        initial_interval: float = 2.0,
        max_interval: float = 30.0,
        backoff_factor: float = 2.0,
    ):
        """Wait until upload preprocessing exposes row_count and run() can start."""
        started_at = time.monotonic()
        interval = initial_interval
        timeout = timeout or self.ingestion_timeout_seconds
        last_status = None

        while True:
            status = await self.get_dataset_status(dataset_id)
            last_status = status.status

            if status.error:
                raise AdaptiveDataError(status.error.message)

            if status.status == "failed":
                raise AdaptiveDataError(
                    f"Dataset ingestion failed for {dataset_id}: {status.error}"
                )

            if status.row_count is not None:
                logger.info(
                    "Dataset %s ingestion ready with %s rows.",
                    dataset_id,
                    status.row_count,
                )
                return status

            if time.monotonic() - started_at >= timeout:
                raise TimeoutError(
                    f"Timed out after {timeout}s waiting for dataset ingestion; "
                    f"last status: {last_status}"
                )

            await asyncio.sleep(interval)
            interval = min(interval * backoff_factor, max_interval)

    @staticmethod
    def _record_prompt(record: Dict[str, Any]) -> str:
        """Builds a stable prompt column for the Adaption augmentation API."""
        for key in ("prompt", "text", "title", "content", "description", "message"):
            value = record.get(key)
            if value:
                return str(value)
        return json.dumps(record, ensure_ascii=False, sort_keys=True)

    @classmethod
    def prepare_record_for_upload(
        cls, record: Dict[str, Any], operation: str
    ) -> Dict[str, Any]:
        """Add SDK-facing prompt/context columns without mutating the source record."""
        sdk_record = dict(record)
        sdk_record["__adaptive_operation"] = operation
        sdk_record["__adaptive_prompt"] = cls._record_prompt(record)
        sdk_record["__source_record"] = json.dumps(
            record, ensure_ascii=False, sort_keys=True
        )
        return sdk_record

    def _write_batch_jsonl(self, batch: List[Dict[str, Any]], operation: str) -> str:
        """Serialize an in-memory batch to the SDK's supported JSONL upload path."""
        tmp = tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".jsonl",
            prefix=f"crisislingua_{operation}_",
            encoding="utf-8",
            delete=False,
        )
        with tmp:
            for record in batch:
                tmp.write(
                    json.dumps(
                        self.prepare_record_for_upload(record, operation),
                        ensure_ascii=False,
                    )
                    + "\n"
                )
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

    @classmethod
    def strip_internal_record(cls, record: Dict[str, Any]) -> Dict[str, Any]:
        return {
            key: value
            for key, value in record.items()
            if key not in cls.INTERNAL_COLUMNS
        }

    @classmethod
    def _strip_internal_columns(
        cls, records: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        return [cls.strip_internal_record(record) for record in records]

    @staticmethod
    def _operation_blueprint(operation: str) -> str:
        blueprints = {
            "full_adaptation": (
                "Process each CrisisLingua-VQA source record in one pass. Keep only "
                "crisis-relevant humanitarian signal, normalize multilingual or "
                "code-switched text into clear response intent, map the report to "
                "FEMA ESF and MIRA-style categories, infer language when evidence "
                "supports it, and preserve useful source context such as title, "
                "content, source_url, media_urls, timestamp, and location fields."
            ),
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

    @staticmethod
    def _recipe_specification(operation: str) -> Dict[str, Any]:
        return {
            "version": "1",
            "recipes": {
                "deduplication": operation in {"filter_noise", "full_adaptation"},
                "prompt_rephrase": operation
                in {"extract_intent", "map_fema_esf", "full_adaptation"},
            },
        }

    @staticmethod
    def _column_mapping() -> Dict[str, Any]:
        return {
            "prompt": "__adaptive_prompt",
            "context": ["__source_record", "__adaptive_operation"],
        }

    async def _upload_file_s3_compatible_async(
        self, path: Path, *, dataset_name: str
    ) -> Dict[str, Any]:
        """
        Upload a JSONL file through the lower-level SDK upload workflow.

        S3 presigned PUT URLs reject HTTP/1.1 chunked request bodies with
        501 NotImplemented. Passing an async generator to httpx creates an
        unknown-length body, so use the shard's bounded bytes and an explicit
        Content-Length instead.
        """
        create_resp = await self._execute_async_with_policy(
            self.async_client.datasets.create,
            source={
                "type": "file",
                "file_format": "jsonl",
                "name": dataset_name,
            },
        )

        if create_resp.upload_instructions is None:
            raise AdaptiveDataError(
                "Server did not return upload instructions for file source"
            )

        file_bytes = await asyncio.to_thread(path.read_bytes)
        file_size = len(file_bytes)
        sha256_hex = hashlib.sha256(file_bytes).hexdigest()

        async with httpx.AsyncClient(timeout=None) as http_client:
            response = await http_client.put(
                create_resp.upload_instructions.url,
                content=file_bytes,
                headers={
                    "Content-Length": str(file_size),
                    "User-Agent": self.user_agent,
                },
            )
            response.raise_for_status()

        complete_resp = await self._execute_async_with_policy(
            self.async_client.datasets.upload.complete_by_id,
            create_resp.dataset_id,
            file_size_bytes=file_size,
            sha256=sha256_hex,
        )
        return {
            "dataset_id": complete_resp.dataset_id,
            "status": complete_resp.status,
            "sha256": sha256_hex,
            "file_size_bytes": file_size,
        }

    async def submit_dataset_job(
        self,
        upload_path: str | os.PathLike[str],
        operation: str,
        *,
        batch_id: str,
        dataset_name: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        max_rows: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Upload a prepared JSONL shard, wait only for ingestion readiness, and
        launch the Adaption run. This deliberately does not wait for completion.
        """
        path = Path(upload_path)
        if not path.exists():
            raise FileNotFoundError(f"Upload shard missing: {path}")

        dataset_name = dataset_name or (
            f"CrisisLingua-VQA-{operation}-{batch_id}-{int(time.time())}"
        )
        upload = await self._upload_file_s3_compatible_async(
            path, dataset_name=dataset_name
        )
        dataset_id = upload["dataset_id"]

        ingestion_status = await self.wait_for_ingestion_ready_async(dataset_id)

        run_key = idempotency_key or (
            f"crisislingua-vqa:{operation}:{batch_id}:{upload['sha256'][:16]}"
        )
        job_specification: Dict[str, Any] = {"idempotency_key": run_key}
        if max_rows is not None:
            job_specification["max_rows"] = float(max_rows)

        run_resp = await self._execute_async_with_policy(
            self.async_client.datasets.run,
            dataset_id,
            column_mapping=self._column_mapping(),
            recipe_specification=self._recipe_specification(operation),
            brand_controls={
                "length": "concise",
                "hallucination_mitigation": True,
                "blueprint": self._operation_blueprint(operation),
            },
            job_specification=job_specification,
        )

        logger.info(
            "Submitted Adaption run for %s (%s rows, dataset_id=%s, run_id=%s).",
            batch_id,
            ingestion_status.row_count,
            dataset_id,
            run_resp.run_id,
        )
        self._reset_failures()
        return {
            "batch_id": batch_id,
            "operation": operation,
            "dataset_id": dataset_id,
            "run_id": run_resp.run_id,
            "row_count": ingestion_status.row_count,
            "status": "running",
            "estimated_minutes": run_resp.estimated_minutes,
            "estimated_credits_consumed": run_resp.estimated_credits_consumed,
            "idempotency_key": run_key,
            "upload_sha256": upload["sha256"],
            "file_size_bytes": upload["file_size_bytes"],
            "submitted_at": time.time(),
        }

    async def get_dataset_status(self, dataset_id: str):
        return await self._execute_async_with_policy(
            self.async_client.datasets.get_status, dataset_id
        )

    async def wait_for_dataset_terminal(
        self,
        dataset_id: str,
        *,
        timeout: Optional[float] = None,
        initial_interval: float = 10.0,
        max_interval: float = 60.0,
        backoff_factor: float = 1.5,
    ):
        """Poll a launched dataset until succeeded or failed."""
        timeout = timeout or self.run_timeout_seconds
        started_at = time.monotonic()
        interval = initial_interval
        last_status = None

        while True:
            status = await self.get_dataset_status(dataset_id)
            last_status = status.status
            if status.status in {"succeeded", "failed"}:
                return status
            if time.monotonic() - started_at >= timeout:
                raise TimeoutError(
                    f"Timed out after {timeout}s waiting for dataset run; "
                    f"last status: {last_status}"
                )
            await asyncio.sleep(interval)
            interval = min(interval * backoff_factor, max_interval)

    async def download_dataset_job(
        self,
        dataset_id: str,
        output_path: str | os.PathLike[str],
        *,
        file_format: str = "jsonl",
    ) -> int:
        """
        Stream processed JSONL to disk, stripping SDK-only columns line by line.

        The SDK download() method returns a full string; streaming prevents memory
        spikes for large completed shards.
        """
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_name(f"{path.name}.tmp")

        while True:
            records_written = 0
            try:
                await self._api_rate_limiter.wait()
                await asyncio.sleep(self._jitter_delay())

                async with self.async_client.datasets.with_streaming_response.download(
                    dataset_id, file_format=file_format
                ) as response:
                    with open(tmp_path, "w", encoding="utf-8") as out_f:
                        async for line in response.iter_lines():
                            if not line.strip():
                                continue
                            record = json.loads(line)
                            out_f.write(
                                json.dumps(
                                    self.strip_internal_record(record),
                                    ensure_ascii=False,
                                )
                                + "\n"
                            )
                            records_written += 1

                os.replace(tmp_path, path)
                self._reset_failures()
                return records_written
            except APIStatusError as e:
                try:
                    tmp_path.unlink()
                except FileNotFoundError:
                    pass

                if e.status_code not in self.RETRYABLE_STATUS_CODES:
                    self._reset_failures()
                    raise

                if e.status_code in self.RATE_LIMIT_STATUS_CODES:
                    failures = self._increment_failures()
                    if failures >= self.circuit_breaker_limit:
                        raise AdaptiveDataError(
                            "Circuit Breaker Tripped (Adaption SDK): "
                            "halting to prevent IP ban"
                        ) from e

                sleep_time = self._retry_after_seconds(e) or random.uniform(5.0, 15.0)
                logger.warning(
                    "Adaption download API %s. Sleeping for %.2fs before retry.",
                    e.status_code,
                    sleep_time,
                )
                await asyncio.sleep(sleep_time)

    async def reshape_file(
        self, upload_path: str | os.PathLike[str], operation: str
    ) -> List[Dict[str, Any]]:
        """Async compatibility helper that submits, waits, downloads, and returns records."""
        batch_id = f"compat-{int(time.time())}"
        job = await self.submit_dataset_job(
            upload_path,
            operation,
            batch_id=batch_id,
            dataset_name=f"CrisisLingua-VQA-{operation}-{batch_id}",
        )
        status = await self.wait_for_dataset_terminal(job["dataset_id"])
        if status.status == "failed":
            message = status.error.message if status.error else "unknown error"
            raise AdaptiveDataError(
                f"Dataset run failed for {job['dataset_id']}: {message}"
            )

        tmp = tempfile.NamedTemporaryFile(
            suffix=".jsonl",
            prefix=f"crisislingua_{operation}_download_",
            delete=False,
        )
        tmp.close()
        try:
            await self.download_dataset_job(job["dataset_id"], tmp.name)
            records: List[Dict[str, Any]] = []
            with open(tmp.name, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        records.append(json.loads(line))
            return records
        finally:
            try:
                os.unlink(tmp.name)
            except OSError:
                logger.warning("Could not remove temporary download file: %s", tmp.name)

    def reshape_batch(
        self, batch: List[Dict[str, Any]], operation: str
    ) -> List[Dict[str, Any]]:
        """
        Legacy synchronous API used by staged components.

        The implementation now uses the async streaming workflow internally.
        """
        logger.info(
            "Processing batch of %s records for operation: '%s'",
            len(batch),
            operation,
        )

        upload_path = None
        try:
            upload_path = self._write_batch_jsonl(batch, operation)
            return run_coroutine_sync(self.reshape_file(upload_path, operation))
        except Exception as e:
            logger.error("Adaptive Data SDK operation failed: %s", e)
            raise AdaptiveDataError(f"Failed to reshape batch via SDK: {e}") from e
        finally:
            if upload_path:
                try:
                    os.unlink(upload_path)
                except OSError:
                    logger.warning(
                        "Could not remove temporary upload file: %s", upload_path
                    )
