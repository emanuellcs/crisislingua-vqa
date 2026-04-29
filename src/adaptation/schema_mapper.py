import asyncio
import hashlib
import json
import logging
import os
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

from .adaptive_client import AdaptiveDataClient, AdaptiveDataError, run_coroutine_sync
from .filter_noise import NoiseFilter
from .intent_extractor import IntentExtractor

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(name)s - %(message)s"
)
logger = logging.getLogger("SchemaMapper")


class AdaptationPipeline:
    """
    Streams raw data through the Adaptive Data pipeline.

    The default single_pass mode dispatches many independent Adaption runs and
    downloads completed shards as they finish. Set ADAPTATION_MODE=staged to use
    the legacy three-operation flow.
    """

    TERMINAL_LOCAL_STATES = {"downloaded", "downloaded_partial", "error"}
    ACTIVE_LOCAL_STATES = {"submitting", "pending", "running", "succeeded", "failed"}

    def __init__(
        self,
        raw_data_paths: List[str],
        output_dir: str = "data/intermediate",
        *,
        batch_records: Optional[int] = None,
        batch_bytes: Optional[int] = None,
        max_inflight_runs: Optional[int] = None,
        mode: Optional[str] = None,
    ):
        self.raw_data_paths = [Path(p) for p in raw_data_paths]
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.output_file = self.output_dir / "reshaped_mapped_reports.jsonl"

        self.mode = (mode or os.getenv("ADAPTATION_MODE", "single_pass")).lower()
        self.operation = os.getenv("ADAPTATION_OPERATION", "full_adaptation")
        self.batch_records = batch_records or int(
            os.getenv("ADAPTATION_BATCH_RECORDS", "5000")
        )
        self.batch_bytes = batch_bytes or int(
            os.getenv("ADAPTATION_BATCH_BYTES", str(32 * 1024 * 1024))
        )
        self.max_inflight_runs = max_inflight_runs or int(
            os.getenv("ADAPTATION_MAX_INFLIGHT_RUNS", "8")
        )
        self.poll_interval_seconds = float(
            os.getenv("ADAPTATION_POLL_INTERVAL_SECONDS", "30")
        )
        self.max_wall_seconds = float(
            os.getenv("ADAPTATION_MAX_WALL_SECONDS", str(11.5 * 60 * 60))
        )
        self.max_job_attempts = int(os.getenv("ADAPTATION_MAX_JOB_ATTEMPTS", "3"))

        self.batch_size = self.batch_records  # legacy attribute compatibility
        self.work_root = self.output_dir / "adaptation_work"
        self.manifest_file = self.output_dir / "adaptation_jobs_manifest.json"
        self._manifest_lock = threading.Lock()
        self.run_dir: Optional[Path] = None

        self.client = AdaptiveDataClient()
        self.noise_filter: Optional[NoiseFilter] = None
        self.intent_extractor: Optional[IntentExtractor] = None
        if self.mode == "staged":
            self.noise_filter = NoiseFilter(client=self.client)
            self.intent_extractor = IntentExtractor(client=self.client)

    def stream_raw_data(self) -> Generator[Dict[str, Any], None, None]:
        """Yields records from raw JSONL files one by one to save memory."""
        for file_path in self.raw_data_paths:
            if not file_path.exists():
                logger.warning("Input file missing: %s. Skipping.", file_path)
                continue

            logger.info("Reading raw stream from %s", file_path)
            with open(file_path, "r", encoding="utf-8") as f:
                for line_number, line in enumerate(f, start=1):
                    if not line.strip():
                        continue
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError as exc:
                        logger.warning(
                            "Skipping malformed JSONL record in %s:%s: %s",
                            file_path,
                            line_number,
                            exc,
                        )

    def process_pipeline(self):
        """Executes the Adaptation process."""
        if self.mode == "staged":
            self._process_pipeline_staged()
            return

        run_coroutine_sync(self._process_pipeline_async())

    def _input_signature(self) -> Dict[str, Any]:
        files = []
        for path in self.raw_data_paths:
            if path.exists():
                stat = path.stat()
                files.append(
                    {
                        "path": str(path),
                        "size": stat.st_size,
                        "mtime_ns": stat.st_mtime_ns,
                    }
                )
            else:
                files.append({"path": str(path), "missing": True})

        payload = {
            "files": files,
            "mode": self.mode,
            "operation": self.operation,
            "batch_records": self.batch_records,
            "batch_bytes": self.batch_bytes,
        }
        digest = hashlib.sha256(
            json.dumps(payload, sort_keys=True).encode("utf-8")
        ).hexdigest()
        return {"hash": digest, "payload": payload}

    def _load_manifest(self) -> Optional[Dict[str, Any]]:
        if not self.manifest_file.exists():
            return None
        with open(self.manifest_file, "r", encoding="utf-8") as f:
            return json.load(f)

    def _save_manifest(self, manifest: Dict[str, Any]):
        manifest["updated_at"] = time.time()
        self.manifest_file.parent.mkdir(parents=True, exist_ok=True)
        with self._manifest_lock:
            tmp = tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self.manifest_file.parent,
                prefix=f"{self.manifest_file.name}.",
                suffix=".tmp",
                delete=False,
            )
            try:
                with tmp:
                    json.dump(manifest, tmp, ensure_ascii=False, indent=2)
                    tmp.write("\n")
                os.replace(tmp.name, self.manifest_file)
            except Exception:
                try:
                    os.unlink(tmp.name)
                except OSError:
                    pass
                raise

    def _manifest_matches(
        self, manifest: Optional[Dict[str, Any]], signature: Dict[str, Any]
    ) -> bool:
        return bool(
            manifest
            and manifest.get("input_signature", {}).get("hash") == signature["hash"]
            and manifest.get("mode") == self.mode
            and manifest.get("operation") == self.operation
        )

    def _load_or_create_manifest(self) -> Dict[str, Any]:
        signature = self._input_signature()
        existing = self._load_manifest()

        if self._manifest_matches(existing, signature):
            self.run_dir = self.work_root / existing["run_id"]
            logger.info(
                "Resuming adaptation manifest %s with %s jobs.",
                self.manifest_file,
                len(existing.get("jobs", [])),
            )
            return existing

        if existing:
            logger.info(
                "Starting a fresh adaptation run because inputs or settings changed."
            )

        run_id = signature["hash"][:12]
        self.run_dir = self.work_root / run_id
        self.run_dir.mkdir(parents=True, exist_ok=True)
        jobs = self._build_upload_shards(self.run_dir)
        manifest = {
            "version": 2,
            "status": "prepared",
            "mode": self.mode,
            "operation": self.operation,
            "run_id": run_id,
            "input_signature": signature,
            "output_file": str(self.output_file),
            "work_dir": str(self.run_dir),
            "created_at": time.time(),
            "jobs": jobs,
        }
        self._save_manifest(manifest)
        return manifest

    def _build_upload_shards(self, run_dir: Path) -> List[Dict[str, Any]]:
        jobs: List[Dict[str, Any]] = []
        shard_index = 0
        shard_file = None
        shard_path: Optional[Path] = None
        shard_records = 0
        shard_bytes = 0
        total_records = 0

        def _open_shard(index: int):
            path = run_dir / f"batch_{index:06d}.jsonl"
            return path, open(path, "w", encoding="utf-8")

        def _close_shard():
            nonlocal shard_file, shard_path, shard_records, shard_bytes, shard_index
            if shard_file is None or shard_path is None:
                return
            shard_file.close()
            batch_id = f"batch-{shard_index:06d}"
            jobs.append(
                {
                    "batch_id": batch_id,
                    "batch_index": shard_index,
                    "state": "prepared",
                    "operation": self.operation,
                    "input_path": str(shard_path),
                    "output_path": str(run_dir / f"{batch_id}.mapped.jsonl"),
                    "record_count": shard_records,
                    "input_bytes": shard_bytes,
                    "attempts": 0,
                    "download_attempts": 0,
                }
            )
            shard_file = None
            shard_path = None
            shard_records = 0
            shard_bytes = 0
            shard_index += 1

        logger.info(
            "Building Adaption upload shards in %s (records<=%s, bytes<=%s).",
            run_dir,
            self.batch_records,
            self.batch_bytes,
        )

        for record in self.stream_raw_data():
            upload_record = self.client.prepare_record_for_upload(
                record, self.operation
            )
            line = json.dumps(upload_record, ensure_ascii=False) + "\n"
            line_bytes = len(line.encode("utf-8"))

            if shard_file is None:
                shard_path, shard_file = _open_shard(shard_index)

            would_exceed_records = shard_records >= self.batch_records
            would_exceed_bytes = (
                shard_records > 0 and shard_bytes + line_bytes > self.batch_bytes
            )
            if would_exceed_records or would_exceed_bytes:
                _close_shard()
                shard_path, shard_file = _open_shard(shard_index)

            shard_file.write(line)
            shard_records += 1
            shard_bytes += line_bytes
            total_records += 1

        _close_shard()
        logger.info(
            "Prepared %s upload shards containing %s source records.",
            len(jobs),
            total_records,
        )
        return jobs

    async def _process_pipeline_async(self):
        manifest = self._load_or_create_manifest()
        jobs = manifest.get("jobs", [])

        if not jobs:
            self._write_empty_output(manifest)
            logger.info("Adaptation Pipeline complete. No records were available.")
            return

        if manifest.get("status") == "complete" and self.output_file.exists():
            logger.info("Adaptation output already complete at %s", self.output_file)
            return

        started_at = time.monotonic()
        submit_tasks: Dict[asyncio.Task, Dict[str, Any]] = {}

        while True:
            self._enforce_wall_clock(started_at, manifest)
            self._start_submit_tasks(manifest, submit_tasks)
            await self._collect_submit_tasks(submit_tasks, timeout=0.1)
            await self._poll_and_download_jobs(manifest)

            if self._all_jobs_terminal(jobs) and not submit_tasks:
                break

            self._log_progress(jobs)
            await asyncio.sleep(self.poll_interval_seconds)

        total_written = self._merge_outputs(manifest)
        logger.info(
            "Adaptation Pipeline complete. Successfully wrote %s reshaped records.",
            total_written,
        )

    def _write_empty_output(self, manifest: Dict[str, Any]):
        tmp_path = self.output_file.with_name(f"{self.output_file.name}.tmp")
        with open(tmp_path, "w", encoding="utf-8"):
            pass
        os.replace(tmp_path, self.output_file)
        manifest["status"] = "complete"
        manifest["output_records"] = 0
        manifest["completed_at"] = time.time()
        self._save_manifest(manifest)

    def _enforce_wall_clock(self, started_at: float, manifest: Dict[str, Any]):
        if not self.max_wall_seconds:
            return
        elapsed = time.monotonic() - started_at
        if elapsed < self.max_wall_seconds:
            return
        manifest["status"] = "paused"
        manifest["pause_reason"] = (
            "Reached ADAPTATION_MAX_WALL_SECONDS before all remote jobs completed. "
            "Rerun Phase 2 to resume from the manifest."
        )
        self._save_manifest(manifest)
        raise TimeoutError(manifest["pause_reason"])

    def _inflight_count(self, jobs: List[Dict[str, Any]]) -> int:
        return sum(1 for job in jobs if job.get("state") in self.ACTIVE_LOCAL_STATES)

    def _start_submit_tasks(
        self,
        manifest: Dict[str, Any],
        submit_tasks: Dict[asyncio.Task, Dict[str, Any]],
    ):
        jobs = manifest.get("jobs", [])
        inflight = self._inflight_count(jobs) + len(submit_tasks)
        available_slots = max(self.max_inflight_runs - inflight, 0)
        if available_slots <= 0:
            return

        for job in jobs:
            if available_slots <= 0:
                break
            if not self._job_needs_submit(job):
                continue
            job["state"] = "submitting"
            job["attempts"] = int(job.get("attempts", 0)) + 1
            job["updated_at"] = time.time()
            self._save_manifest(manifest)
            task = asyncio.create_task(self._submit_job(job, manifest))
            submit_tasks[task] = job
            available_slots -= 1

    @staticmethod
    def _job_needs_submit(job: Dict[str, Any]) -> bool:
        return not job.get("dataset_id") and job.get("state") in {
            "prepared",
            "submit_failed",
        }

    async def _collect_submit_tasks(
        self,
        submit_tasks: Dict[asyncio.Task, Dict[str, Any]],
        *,
        timeout: float,
    ):
        if not submit_tasks:
            return

        done = {task for task in submit_tasks if task.done()}
        if not done:
            done, _ = await asyncio.wait(
                submit_tasks.keys(),
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )

        for task in done:
            job = submit_tasks.pop(task)
            try:
                await task
            except Exception as exc:
                logger.error("Submit task crashed for %s: %s", job["batch_id"], exc)

    async def _submit_job(self, job: Dict[str, Any], manifest: Dict[str, Any]):
        try:
            submitted = await self.client.submit_dataset_job(
                job["input_path"],
                job["operation"],
                batch_id=job["batch_id"],
                dataset_name=f"CrisisLingua-VQA-{job['operation']}-{job['batch_id']}",
                idempotency_key=self._job_idempotency_key(manifest, job),
                max_rows=job.get("record_count"),
            )
            job.update(submitted)
            job["state"] = "running"
            job["error"] = None
        except Exception as exc:
            logger.error("Failed to submit %s: %s", job["batch_id"], exc)
            job["error"] = str(exc)
            if int(job.get("attempts", 0)) >= self.max_job_attempts:
                job["state"] = "error"
            else:
                job["state"] = "submit_failed"
        finally:
            job["updated_at"] = time.time()
            self._save_manifest(manifest)

    @staticmethod
    def _job_idempotency_key(manifest: Dict[str, Any], job: Dict[str, Any]) -> str:
        return f"crisislingua-vqa:{manifest['run_id']}:{job['batch_id']}"

    async def _poll_and_download_jobs(self, manifest: Dict[str, Any]):
        pollable = [
            job
            for job in manifest.get("jobs", [])
            if job.get("dataset_id")
            and job.get("state") in {"pending", "running", "succeeded", "failed"}
        ]
        if not pollable:
            return
        await asyncio.gather(
            *(self._refresh_and_download_job(job, manifest) for job in pollable)
        )

    async def _refresh_and_download_job(
        self, job: Dict[str, Any], manifest: Dict[str, Any]
    ):
        try:
            status = await self.client.get_dataset_status(job["dataset_id"])
            job["remote_status"] = status.status
            job["row_count"] = status.row_count
            if status.progress:
                job["progress"] = {
                    "percent": status.progress.percent,
                    "processed_rows": status.progress.processed_rows,
                    "total_rows": status.progress.total_rows,
                }

            if status.status in {"pending", "running"}:
                job["state"] = status.status
                job["updated_at"] = time.time()
                self._save_manifest(manifest)
                return

            if status.status == "failed":
                job["terminal_status"] = "failed"
                job["error"] = status.error.message if status.error else "failed"
                logger.warning(
                    "Adaption job %s failed; attempting partial download.",
                    job["batch_id"],
                )
            else:
                job["terminal_status"] = "succeeded"
                job["error"] = None

            await self._download_job_output(job, manifest)
        except Exception as exc:
            job["error"] = str(exc)
            job["poll_attempts"] = int(job.get("poll_attempts", 0)) + 1
            if job["poll_attempts"] >= self.max_job_attempts:
                logger.error("Marking %s as error: %s", job["batch_id"], exc)
                job["state"] = "error"
            job["updated_at"] = time.time()
            self._save_manifest(manifest)

    async def _download_job_output(self, job: Dict[str, Any], manifest: Dict[str, Any]):
        output_path = Path(job["output_path"])
        if output_path.exists() and output_path.stat().st_size >= 0:
            records_written = self._count_jsonl_records(output_path)
        else:
            job["state"] = job.get("terminal_status", "succeeded")
            job["download_attempts"] = int(job.get("download_attempts", 0)) + 1
            self._save_manifest(manifest)
            records_written = await self.client.download_dataset_job(
                job["dataset_id"], output_path
            )

        job["output_records"] = records_written
        job["state"] = (
            "downloaded_partial"
            if job.get("terminal_status") == "failed"
            else "downloaded"
        )
        job["downloaded_at"] = time.time()
        job["updated_at"] = time.time()
        self._save_manifest(manifest)
        logger.info(
            "Downloaded %s records for %s.",
            records_written,
            job["batch_id"],
        )

    @staticmethod
    def _count_jsonl_records(path: Path) -> int:
        count = 0
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    count += 1
        return count

    def _all_jobs_terminal(self, jobs: List[Dict[str, Any]]) -> bool:
        return all(job.get("state") in self.TERMINAL_LOCAL_STATES for job in jobs)

    def _log_progress(self, jobs: List[Dict[str, Any]]):
        total = len(jobs)
        downloaded = sum(
            1
            for job in jobs
            if job.get("state") in {"downloaded", "downloaded_partial"}
        )
        errored = sum(1 for job in jobs if job.get("state") == "error")
        running = sum(1 for job in jobs if job.get("state") in self.ACTIVE_LOCAL_STATES)
        prepared = sum(1 for job in jobs if self._job_needs_submit(job))
        logger.info(
            "Adaptation progress: %s/%s downloaded, %s running, %s queued, %s errors.",
            downloaded,
            total,
            running,
            prepared,
            errored,
        )

    def _merge_outputs(self, manifest: Dict[str, Any]) -> int:
        jobs = sorted(manifest.get("jobs", []), key=lambda item: item["batch_index"])
        tmp_path = self.output_file.with_name(f"{self.output_file.name}.tmp")
        records_written = 0
        failed_batches = 0

        with open(tmp_path, "w", encoding="utf-8") as out_f:
            for job in jobs:
                if job.get("state") == "error":
                    failed_batches += 1
                    logger.error(
                        "Skipping failed adaptation shard %s: %s",
                        job["batch_id"],
                        job.get("error"),
                    )
                    continue
                if job.get("state") not in {"downloaded", "downloaded_partial"}:
                    continue

                output_path = Path(job["output_path"])
                if not output_path.exists():
                    failed_batches += 1
                    logger.error(
                        "Downloaded shard missing for %s: %s",
                        job["batch_id"],
                        output_path,
                    )
                    continue

                with open(output_path, "r", encoding="utf-8") as in_f:
                    for line in in_f:
                        if line.strip():
                            out_f.write(line)
                            records_written += 1

        os.replace(tmp_path, self.output_file)
        manifest["status"] = "complete"
        manifest["output_records"] = records_written
        manifest["failed_batches"] = failed_batches
        manifest["completed_at"] = time.time()
        self._save_manifest(manifest)

        if failed_batches:
            logger.warning(
                "Merged adaptation output with %s failed shard(s).", failed_batches
            )
        return records_written

    def _process_pipeline_staged(self):
        """Legacy exact three-stage behavior for compatibility/debugging."""
        if self.noise_filter is None or self.intent_extractor is None:
            self.noise_filter = NoiseFilter(client=self.client)
            self.intent_extractor = IntentExtractor(client=self.client)

        current_batch: List[Dict[str, Any]] = []
        total_written = 0

        with open(self.output_file, "w", encoding="utf-8") as out_f:
            for record in self.stream_raw_data():
                current_batch.append(record)

                if len(current_batch) >= self.batch_records:
                    total_written += self._execute_batch_and_write(current_batch, out_f)
                    current_batch = []

            if current_batch:
                total_written += self._execute_batch_and_write(current_batch, out_f)

        logger.info(
            "Staged Adaptation Pipeline complete. Successfully wrote %s records.",
            total_written,
        )

    def _execute_batch_and_write(self, batch: List[Dict[str, Any]], file_obj) -> int:
        """Passes a batch sequentially through the legacy modular endpoints."""
        try:
            if self.noise_filter is None or self.intent_extractor is None:
                raise AdaptiveDataError("Staged components are not initialized.")

            filtered_batch = self.noise_filter.process_batch(batch)
            intent_batch = self.intent_extractor.process_batch(filtered_batch)
            mapped_batch = self.client.reshape_batch(
                intent_batch, operation="map_fema_esf"
            )

            for record in mapped_batch:
                file_obj.write(json.dumps(record, ensure_ascii=False) + "\n")
            return len(mapped_batch)

        except Exception as e:
            logger.error(
                "Batch processing failed. Dropping batch to preserve pipeline "
                "stability: %s",
                e,
            )
            return 0


if __name__ == "__main__":
    input_files = [
        "data/raw/ushahidi_raw_reports.jsonl",
        "data/raw/multimodal_news_scrape.jsonl",
    ]
    pipeline = AdaptationPipeline(raw_data_paths=input_files)
    pipeline.process_pipeline()
