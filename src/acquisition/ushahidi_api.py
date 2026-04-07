import os
import json
import logging
from pathlib import Path
from typing import Dict, Any, Generator
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

# Load environment variables (e.g., API keys, target URLs)
load_dotenv()

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - [%(levelname)s] - %(name)s - %(message)s'
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
            raise ValueError("USHAHIDI_DEPLOYMENT_URLS environment variable is not set.")
        self.base_urls = [url.strip() for url in target_urls.split(",")]
        
        # Configure a robust session with exponential backoff
        self.session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

    def stream_deployment_posts(self, base_url: str) -> Generator[Dict[str, Any], None, None]:
        """
        Paginates through an Ushahidi deployment, yielding raw posts one by one
        to prevent memory exhaustion on massive datasets.
        """
        endpoint = f"{base_url.rstrip('/')}/api/v3/posts"
        limit = 100
        offset = 0
        total_fetched = 0

        logger.info(f"Initiating extraction from deployment: {endpoint}")

        while True:
            params = {"limit": limit, "offset": offset}
            try:
                response = self.session.get(endpoint, params=params, timeout=15)
                response.raise_for_status()
                data = response.json()
            except requests.exceptions.RequestException as e:
                logger.error(f"Critical failure connecting to {endpoint}: {e}")
                break

            results = data.get("results", [])
            if not results:
                logger.info(f"Reached end of pagination for {base_url}. Total fetched: {total_fetched}")
                break

            for post in results:
                # We yield the raw post object directly to be saved
                yield post
                total_fetched += 1

            offset += limit

    def execute_pipeline(self):
        """Executes the extraction across all deployments and streams to JSONL."""
        logger.info(f"Starting acquisition pipeline. Target file: {self.output_file}")
        
        # Open in append mode so if the script crashes, data isn't lost
        with open(self.output_file, 'a', encoding='utf-8') as f:
            for url in self.base_urls:
                for raw_post in self.stream_deployment_posts(url):
                    # Write immediately to disk
                    f.write(json.dumps(raw_post, ensure_ascii=False) + '\n')
                    
        logger.info("Ushahidi acquisition pipeline completed successfully.")

if __name__ == "__main__":
    fetcher = UshahidiArchiveFetcher()
    fetcher.execute_pipeline()