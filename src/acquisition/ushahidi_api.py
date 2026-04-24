import os
import sys
import time
import json
import random
import logging
from pathlib import Path
from typing import Dict, Any, Generator
import requests
from dotenv import load_dotenv

# Load environment variables (e.g., API keys, target URLs)
load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(name)s - %(message)s"
)
logger = logging.getLogger("UshahidiAcquisition")


class UshahidiArchiveFetcher:
    """
    Production client for the Ushahidi V3 API.
    Handles pagination, rate limiting, and exponential backoff.
    """

    def __init__(self, output_dir: str = "../../data/raw"):
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
        self.session.headers.update({"User-Agent": self.user_agent})

    def _safe_get(self, url: str, **kwargs) -> requests.Response:
        """Executes a GET request with circuit breaker and Retry-After parsing."""
        while True:
            try:
                # Add mandatory polite delay before every single fetch
                time.sleep(random.uniform(3.0, 8.0))

                response = self.session.get(url, **kwargs)

                if response.status_code in (403, 429):
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

    def get_categories(self, base_url: str) -> Dict[int, str]:
        """Fetches survey categories to map IDs to names for downstream ESF mapping."""
        endpoint = f"{base_url.rstrip('/')}/api/v3/categories"
        try:
            response = self._safe_get(endpoint, timeout=15)
            data = response.json()
            return {cat["id"]: cat["name"] for cat in data.get("results", [])}
        except Exception as e:
            logger.warning(f"Could not fetch categories from {base_url}: {e}")
            return {}

    def get_media_url(self, base_url: str, media_id: int) -> str:
        """Resolves a media ID to a direct image URL for multimodal VQA."""
        endpoint = f"{base_url.rstrip('/')}/api/v3/media/{media_id}"
        try:
            response = self._safe_get(endpoint, timeout=10)
            data = response.json()
            return data.get("url", "")
        except Exception as e:
            logger.warning(f"Could not fetch media {media_id}: {e}")
            return ""

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
                response = self._safe_get(endpoint, params=params, timeout=15)
                data = response.json()
            except Exception as e:
                logger.error(f"Critical failure connecting to {endpoint}: {e}")
                break

            results = data.get("results", [])
            if not results:
                logger.info(
                    f"Reached end of pagination for {base_url}. Total fetched: {total_fetched}"
                )
                break

            for post in results:
                # Eagerly map category names
                post_cat_ids = post.get("categories", [])
                post["category_names"] = [
                    categories.get(cid, f"Unknown({cid})") for cid in post_cat_ids
                ]

                # Resolve media URLs for VQA pairs
                media_urls = []
                for media_ref in post.get("media", []):
                    m_id = (
                        media_ref if isinstance(media_ref, int) else media_ref.get("id")
                    )
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
