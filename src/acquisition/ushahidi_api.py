import os
import sys
import time
import json
import random
import logging
from pathlib import Path
from typing import Dict, Any, Generator, List, Optional, Tuple, Union
from urllib.parse import quote
import requests
from dotenv import load_dotenv

# Load environment variables (e.g., API keys, target URLs)
load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(name)s - %(message)s"
)
logger = logging.getLogger("UshahidiAcquisition")


class NonJSONResponseError(Exception):
    """Raised when an endpoint responds with HTML or another non-JSON payload."""


class UshahidiArchiveFetcher:
    """
    Production client for the Ushahidi V3 API.
    Handles pagination, rate limiting, and exponential backoff.
    """

    def __init__(self, output_dir: str = "data/raw"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.output_file = self.output_dir / "ushahidi_raw_reports.jsonl"

        # Pull the target deployments from environment variables, comma-separated
        target_urls = os.getenv("USHAHIDI_DEPLOYMENT_URLS")
        if not target_urls:
            raise ValueError(
                "USHAHIDI_DEPLOYMENT_URLS environment variable is not set."
            )
        self.base_urls = [url.strip() for url in target_urls.split(",")]

        # Ethical, transparent bot User-Agent string
        self.user_agent = "CrisisLingua-Research-Bot/1.0 (Uncharted Data Challenge; Research purpose; limit 20req/min)"
        self.consecutive_failures = 0
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": self.user_agent,
                "Accept": "application/json",
            }
        )

    def _safe_get(self, url: str, **kwargs) -> requests.Response:
        """Executes a GET request with circuit breaker and Retry-After parsing."""
        allow_blocked_response = kwargs.pop("allow_blocked_response", False)
        while True:
            try:
                # Add mandatory polite delay before every single fetch
                time.sleep(random.uniform(3.0, 8.0))

                response = self.session.get(url, **kwargs)

                if response.status_code in (403, 429):
                    if allow_blocked_response and response.status_code == 403:
                        logger.warning(
                            f"Received 403 from {url}; returning response for fallback handling."
                        )
                        return response

                    self.consecutive_failures += 1
                    if self.consecutive_failures >= 3:
                        logger.critical(
                            "Circuit Breaker Tripped: Halting to prevent IP ban"
                        )
                        sys.exit(1)

                    retry_after = response.headers.get("Retry-After")
                    if retry_after and retry_after.isdigit():
                        sleep_time = int(retry_after)
                        logger.warning(
                            f"Received {response.status_code}. Sleeping for {sleep_time}s as per Retry-After."
                        )
                        time.sleep(sleep_time)
                    else:
                        sleep_time = random.uniform(
                            5.0, 15.0
                        )  # Slightly longer on rate limit
                        logger.warning(
                            f"Received {response.status_code}. Sleeping for {sleep_time:.2f}s before retry."
                        )
                        time.sleep(sleep_time)
                    continue

                # Reset consecutive failures on success
                self.consecutive_failures = 0
                response.raise_for_status()
                return response

            except requests.exceptions.RequestException as e:
                # If we get a response but it's 403 or 429, the loop will handle it
                if (
                    hasattr(e, "response")
                    and e.response is not None
                    and e.response.status_code in (403, 429)
                ):
                    continue
                logger.error(f"Network request failed for {url}: {e}")
                raise

    @staticmethod
    def _looks_like_json(response: requests.Response) -> bool:
        content_type = response.headers.get("Content-Type", "").lower()
        body_start = response.text.lstrip()[:1]
        return "json" in content_type or body_start in {"{", "["}

    def _fetch_json(self, url: str, **kwargs) -> Union[Dict[str, Any], List[Any]]:
        response = self._safe_get(url, allow_blocked_response=True, **kwargs)
        if response.status_code >= 400:
            raise NonJSONResponseError(
                f"{url} returned blocked/error status {response.status_code}"
            )
        if not self._looks_like_json(response):
            snippet = response.text[:160].replace("\n", " ")
            raise NonJSONResponseError(
                f"{url} returned {response.status_code} {response.headers.get('Content-Type', '')}: {snippet}"
            )
        try:
            return response.json()
        except ValueError as exc:
            raise NonJSONResponseError(f"{url} returned invalid JSON: {exc}") from exc

    @staticmethod
    def _result_items(data: Union[Dict[str, Any], List[Any]]) -> List[Dict[str, Any]]:
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        results = data.get("results", []) if isinstance(data, dict) else []
        if isinstance(results, list):
            return [item for item in results if isinstance(item, dict)]
        return []

    def _wayback_snapshots(
        self, api_endpoint: str, limit: int = 8
    ) -> List[Tuple[str, str]]:
        """Find archived JSON snapshots for an API endpoint when the live site is blocked."""
        cdx_endpoint = "https://web.archive.org/cdx"
        params = {
            "url": f"{api_endpoint}*",
            "output": "json",
            "filter": ["statuscode:200", "mimetype:application/json"],
            "collapse": "digest",
            "fl": "timestamp,original",
            "limit": str(limit),
            "sort": "reverse",
        }
        try:
            data = self._fetch_json(cdx_endpoint, params=params, timeout=20)
        except Exception as exc:
            logger.warning(
                f"Could not query Internet Archive for {api_endpoint}: {exc}"
            )
            return []

        rows = data[1:] if isinstance(data, list) and data else []
        snapshots = []
        for row in rows:
            if isinstance(row, list) and len(row) >= 2:
                snapshots.append((str(row[0]), str(row[1])))
            elif isinstance(row, dict) and row.get("timestamp") and row.get("original"):
                snapshots.append((str(row["timestamp"]), str(row["original"])))
        return snapshots

    def _fetch_wayback_json(
        self, timestamp: str, original_url: str
    ) -> Union[Dict[str, Any], List[Any]]:
        archived_url = f"https://web.archive.org/web/{timestamp}id_/{quote(original_url, safe=':/?&=%')}"
        return self._fetch_json(archived_url, timeout=30)

    @staticmethod
    def _category_id(category_ref: Any) -> Optional[int]:
        if isinstance(category_ref, int):
            return category_ref
        if isinstance(category_ref, dict) and category_ref.get("id") is not None:
            try:
                return int(category_ref["id"])
            except (TypeError, ValueError):
                return None
        return None

    @staticmethod
    def _media_id(media_ref: Any) -> Optional[int]:
        if isinstance(media_ref, int):
            return media_ref
        if isinstance(media_ref, dict):
            value = media_ref.get("id")
            if value is not None:
                try:
                    return int(value)
                except (TypeError, ValueError):
                    return None
        return None

    @staticmethod
    def _media_url_from_ref(media_ref: Any) -> str:
        if not isinstance(media_ref, dict):
            return ""
        for key in ("url", "original_file_url", "original_url", "file_url", "src"):
            if media_ref.get(key):
                return str(media_ref[key])
        return ""

    def get_categories(self, base_url: str) -> Dict[int, str]:
        """Fetches survey categories to map IDs to names for downstream ESF mapping."""
        endpoint = f"{base_url.rstrip('/')}/api/v3/categories"
        try:
            data = self._fetch_json(endpoint, timeout=15)
            return {cat["id"]: cat["name"] for cat in data.get("results", [])}
        except NonJSONResponseError as exc:
            logger.warning(
                f"Categories endpoint blocked/non-JSON for {base_url}: {exc}. Trying archived API snapshots."
            )
            for timestamp, original_url in self._wayback_snapshots(endpoint, limit=3):
                try:
                    data = self._fetch_wayback_json(timestamp, original_url)
                    categories = {
                        cat["id"]: cat["name"] for cat in self._result_items(data)
                    }
                    if categories:
                        return categories
                except Exception as archive_exc:
                    logger.warning(
                        f"Archived categories snapshot failed for {original_url}: {archive_exc}"
                    )
            return {}
        except Exception as e:
            logger.warning(f"Could not fetch categories from {base_url}: {e}")
            return {}

    def get_media_url(self, base_url: str, media_id: int) -> str:
        """Resolves a media ID to a direct image URL for multimodal VQA."""
        endpoint = f"{base_url.rstrip('/')}/api/v3/media/{media_id}"
        try:
            data = self._fetch_json(endpoint, timeout=10)
            return data.get("url", "")
        except NonJSONResponseError:
            logger.warning(f"Media endpoint {endpoint} returned non-JSON response.")
            return ""
        except Exception as e:
            logger.warning(f"Could not fetch media {media_id}: {e}")
            return ""

    def _stream_wayback_posts(
        self,
        endpoint: str,
        categories: Dict[int, str],
    ) -> Generator[Dict[str, Any], None, None]:
        """Streams archived Ushahidi API JSON when the live deployment blocks API clients."""
        seen_ids = set()
        snapshots = self._wayback_snapshots(endpoint, limit=12)
        if not snapshots:
            logger.warning(f"No archived JSON API snapshots found for {endpoint}.")
            return

        for timestamp, original_url in snapshots:
            try:
                data = self._fetch_wayback_json(timestamp, original_url)
            except Exception as exc:
                logger.warning(
                    f"Archived post snapshot failed for {original_url}: {exc}"
                )
                continue

            for post in self._result_items(data):
                post_id = (
                    post.get("id")
                    or post.get("url")
                    or json.dumps(post, sort_keys=True, default=str)
                )
                if post_id in seen_ids:
                    continue
                seen_ids.add(post_id)
                post["_acquisition_source"] = "internet_archive_ushahidi_api"
                post["_archive_timestamp"] = timestamp
                post["_archive_original_url"] = original_url

                post_cat_ids = [
                    cat_id
                    for cat_id in (
                        self._category_id(ref) for ref in post.get("categories", [])
                    )
                    if cat_id is not None
                ]
                post["category_names"] = [
                    categories.get(cat_id, f"Unknown({cat_id})")
                    for cat_id in post_cat_ids
                ]

                media_urls = []
                for media_ref in post.get("media", []):
                    embedded_url = self._media_url_from_ref(media_ref)
                    if embedded_url:
                        media_urls.append(embedded_url)
                post["media_urls"] = media_urls
                yield post

    def stream_deployment_posts(
        self, base_url: str
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Paginates through an Ushahidi deployment, yielding raw posts with
        resolved media and categories.
        """
        endpoint = f"{base_url.rstrip('/')}/api/v3/posts"
        limit = 500
        offset = 0
        total_fetched = 0

        logger.info(f"Initiating extraction from deployment: {endpoint}")

        # Prefetch categories for eager mapping
        categories = self.get_categories(base_url)

        while True:
            params = {
                "limit": limit,
                "offset": offset,
                "status": "published",
                "tag": "disaster|earthquake|flood",
            }
            try:
                # We use _safe_get which already handles the 3-8s jitter
                data = self._fetch_json(endpoint, params=params, timeout=15)
            except NonJSONResponseError as e:
                logger.error(
                    f"Endpoint {endpoint} returned non-JSON response (likely HTML/Cloudflare): {e}. "
                    "Falling back to archived API snapshots."
                )
                archive_count = 0
                for post in self._stream_wayback_posts(endpoint, categories):
                    yield post
                    archive_count += 1
                logger.info(
                    f"Archived fallback complete for {base_url}. Total fetched: {archive_count}"
                )
                break
            except Exception as e:
                logger.error(f"Critical failure connecting to {endpoint}: {e}")
                break

            results = self._result_items(data)
            if not results:
                logger.info(
                    f"Reached end of pagination for {base_url}. Total fetched: {total_fetched}"
                )
                break

            for post in results:
                # Eagerly map category names
                post_cat_ids = [
                    cat_id
                    for cat_id in (
                        self._category_id(ref) for ref in post.get("categories", [])
                    )
                    if cat_id is not None
                ]
                post["category_names"] = [
                    categories.get(cid, f"Unknown({cid})") for cid in post_cat_ids
                ]

                # Resolve media URLs for VQA pairs
                media_urls = []
                for media_ref in post.get("media", []):
                    embedded_url = self._media_url_from_ref(media_ref)
                    if embedded_url:
                        media_urls.append(embedded_url)
                        continue
                    m_id = self._media_id(media_ref)
                    if m_id:
                        # get_media_url also uses _safe_get with jitter
                        m_url = self.get_media_url(base_url, m_id)
                        if m_url:
                            media_urls.append(m_url)
                post["media_urls"] = media_urls

                yield post
                total_fetched += 1

            offset += limit

    def execute_pipeline(self):
        """Executes the extraction across all deployments and streams to JSONL."""
        logger.info(f"Starting acquisition pipeline. Target file: {self.output_file}")

        # Open in append mode so if the script crashes, data isn't lost
        with open(self.output_file, "a", encoding="utf-8") as f:
            for url in self.base_urls:
                for raw_post in self.stream_deployment_posts(url):
                    # Write immediately to disk
                    f.write(json.dumps(raw_post, ensure_ascii=False) + "\n")

        logger.info("Ushahidi acquisition pipeline completed successfully.")


if __name__ == "__main__":
    fetcher = UshahidiArchiveFetcher()
    fetcher.execute_pipeline()
