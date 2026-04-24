import os
import sys
import json
import time
import random
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
import requests
from bs4 import BeautifulSoup
from tenacity import (
    retry,
    wait_exponential,
    stop_after_attempt,
    retry_if_exception_type,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(name)s - %(message)s"
)
logger = logging.getLogger("MultimodalScraper")


class MultimodalNewsScraper:
    """
    Production scraper that streams Common Crawl (CC-NEWS) data to discover
    disaster-relevant news articles, extracting paired visual and textual data.
    Uses Direct WARC Streaming to bypass domain-specific limitations.
    """

    def __init__(
        self,
        heuristics_path: str = "../../data/intermediate/twb_heuristic_keywords.json",
        output_dir: str = "../../data/raw",
    ):
        self.heuristics_file = Path(heuristics_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.output_file = self.output_dir / "multimodal_news_scrape.jsonl"

        if not self.heuristics_file.exists():
            raise FileNotFoundError(
                f"Heuristics file missing at {self.heuristics_file}. Run twb_glossary.py first."
            )

        # Ethical, transparent bot User-Agent string
        self.user_agent = "CrisisLingua-Research-Bot/1.0 (Uncharted Data Challenge; Research purpose; limit 20req/min)"
        self.consecutive_failures = 0
        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": self.user_agent, "Accept-Encoding": "gzip"}
        )

    def _safe_get(self, url: str, stream: bool = False, **kwargs) -> requests.Response:
        """Executes a GET request with circuit breaker and Retry-After parsing."""
        while True:
            try:
                # Mandatory polite jitter before every fetch
                time.sleep(random.uniform(3.0, 8.0))

                response = self.session.get(url, stream=stream, **kwargs)

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
                        sleep_time = random.uniform(5.0, 15.0)
                        logger.warning(
                            f"Received {response.status_code}. Sleeping for {sleep_time:.2f}s before retry."
                        )
                        time.sleep(sleep_time)
                    continue

                self.consecutive_failures = 0
                response.raise_for_status()
                return response

            except requests.exceptions.RequestException as e:
                if (
                    hasattr(e, "response")
                    and e.response is not None
                    and e.response.status_code in (403, 429)
                ):
                    continue
                logger.error(f"Network request failed for {url}: {e}")
                raise

    def load_heuristics(self) -> List[str]:
        """Loads and flattens localized terminology for global content filtering."""
        logger.info(f"Loading heuristic keywords from {self.heuristics_file}")
        with open(self.heuristics_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        keywords = data.get("keywords", {})
        # Flatten all language keywords into a single search set
        return list(set([k.lower() for lang in keywords.values() for k in lang]))

    @retry(
        wait=wait_exponential(multiplier=2, min=4, max=20),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(requests.exceptions.RequestException),
    )
    def fetch_cc_news_paths(self) -> List[str]:
        """
        Fetches the latest CC-NEWS WARC paths from the Common Crawl index.
        """
        # We target the latest news segments
        current_date = time.strftime("%Y/%m")
        index_url = f"https://data.commoncrawl.org/crawl-data/CC-NEWS/{current_date}/warc.paths.gz"
        logger.info(f"Fetching CC-NEWS paths from {index_url}")

        # We use session.get directly here as tenacity handles retries,
        # but _safe_get is better for circuit breaking.
        response = self._safe_get(index_url, timeout=20)

        import gzip

        decompressed = gzip.decompress(response.content).decode("utf-8")
        paths = [
            f"https://data.commoncrawl.org/{p.strip()}"
            for p in decompressed.splitlines()
            if p.strip()
        ]
        return paths[:5]  # Limit to top 5 segments for this session

    def extract_vqa_data(self, html: str, url: str) -> Optional[Dict[str, Any]]:
        """Parses HTML for OpenGraph metadata to build VQA pairs."""
        soup = BeautifulSoup(html, "html.parser")

        og_image = soup.find("meta", property="og:image")
        og_desc = soup.find("meta", property="og:description")
        og_title = soup.find("meta", property="og:title")

        if not og_image or not og_desc:
            return None

        return {
            "source_url": url,
            "source_type": "common_crawl_news",
            "content": og_desc.get("content", ""),
            "title": og_title.get("content", "") if og_title else "",
            "media_urls": [og_image.get("content", "")],
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }

    def execute_pipeline(self):
        """Streams CC-NEWS WARC segments and filters for disaster content."""
        keywords = self.load_heuristics()
        warc_paths = self.fetch_cc_news_paths()

        total_extracted = 0
        logger.info(
            f"Starting Multimodal acquisition via Common Crawl. Output: {self.output_file}"
        )

        # We need warcio for memory-efficient WARC streaming
        try:
            import warcio
        except ImportError:
            logger.error(
                "The 'warcio' package is required for this pipeline. Install via pip."
            )
            return

        with open(self.output_file, "a", encoding="utf-8") as f:
            for warc_url in warc_paths:
                logger.info(f"Streaming segment: {warc_url}")
                try:
                    response = self._safe_get(warc_url, stream=True, timeout=120)
                    for record in warcio.ArchiveIterator(response.raw):
                        if record.rec_type == "response":
                            # Check content-type to ensure we only parse HTML
                            content_type = record.http_headers.get_header(
                                "Content-Type", ""
                            )
                            if "text/html" not in content_type:
                                continue

                            html_bytes = record.content_stream().read()
                            html_text = html_bytes.decode(
                                "utf-8", errors="ignore"
                            ).lower()

                            # Content-based heuristic filtering
                            if any(kw in html_text for kw in keywords):
                                url = record.rec_headers.get_header("WARC-Target-URI")
                                # Note: extract_vqa_data uses BeautifulSoup on memory payload
                                data = self.extract_vqa_data(html_bytes, url)
                                if data:
                                    f.write(json.dumps(data, ensure_ascii=False) + "\n")
                                    total_extracted += 1

                except Exception as e:
                    logger.warning(f"Failed to process segment {warc_url}: {e}")

        logger.info(
            f"Common Crawl acquisition complete. Extracted {total_extracted} disaster-relevant pairs."
        )


if __name__ == "__main__":
    scraper = MultimodalNewsScraper()
    scraper.execute_pipeline()
