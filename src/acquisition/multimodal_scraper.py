import os
import json
import time
import random
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
import requests
from bs4 import BeautifulSoup
import feedparser
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - [%(levelname)s] - %(name)s - %(message)s'
)
logger = logging.getLogger("MultimodalScraper")

# Pool of User-Agents to prevent basic rate-limiting/blocking from news sites
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0"
]

class MultimodalNewsScraper:
    """
    Production scraper that streams Common Crawl (CC-NEWS) data to discover 
    disaster-relevant news articles, extracting paired visual and textual data.
    Uses Direct WARC Streaming to bypass domain-specific limitations.
    """
    
    def __init__(self, heuristics_path: str = "../../data/intermediate/twb_heuristic_keywords.json", 
                 output_dir: str = "../../data/raw"):
        self.heuristics_file = Path(heuristics_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.output_file = self.output_dir / "multimodal_news_scrape.jsonl"
        
        if not self.heuristics_file.exists():
            raise FileNotFoundError(f"Heuristics file missing at {self.heuristics_file}. Run twb_glossary.py first.")
            
        self.session = requests.Session()
        # Common Crawl specific headers
        self.session.headers.update({
            "User-Agent": random.choice(USER_AGENTS),
            "Accept-Encoding": "gzip"
        })

    def load_heuristics(self) -> List[str]:
        """Loads and flattens localized terminology for global content filtering."""
        logger.info(f"Loading heuristic keywords from {self.heuristics_file}")
        with open(self.heuristics_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        keywords = data.get("keywords", {})
        # Flatten all language keywords into a single search set
        return list(set([k.lower() for lang in keywords.values() for k in lang]))

    @retry(
        wait=wait_exponential(multiplier=2, min=4, max=20),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(requests.exceptions.RequestException)
    )
    def fetch_cc_news_paths(self) -> List[str]:
        """
        Fetches the latest CC-NEWS WARC paths from the Common Crawl index.
        """
        # We target the latest news segments
        current_date = time.strftime("%Y/%m")
        index_url = f"https://data.commoncrawl.org/crawl-data/CC-NEWS/{current_date}/warc.paths.gz"
        logger.info(f"Fetching CC-NEWS paths from {index_url}")
        
        response = self.session.get(index_url, timeout=20)
        response.raise_for_status()
        
        import gzip
        decompressed = gzip.decompress(response.content).decode('utf-8')
        paths = [f"https://data.commoncrawl.org/{p.strip()}" for p in decompressed.splitlines() if p.strip()]
        return paths[:5] # Limit to top 5 segments for this session

    def extract_vqa_data(self, html: str, url: str) -> Optional[Dict[str, Any]]:
        """Parses HTML for OpenGraph metadata to build VQA pairs."""
        soup = BeautifulSoup(html, 'html.parser')
        
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
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }

    def execute_pipeline(self):
        """Streams CC-NEWS WARC segments and filters for disaster content."""
        keywords = self.load_heuristics()
        warc_paths = self.fetch_cc_news_paths()
        
        total_extracted = 0
        logger.info(f"Starting Multimodal acquisition via Common Crawl. Output: {self.output_file}")
        
        # We need warcio for memory-efficient WARC streaming
        try:
            import warcio
        except ImportError:
            logger.error("The 'warcio' package is required for this pipeline. Install via pip.")
            return

        with open(self.output_file, 'a', encoding='utf-8') as f:
            for warc_url in warc_paths:
                logger.info(f"Streaming segment: {warc_url}")
                try:
                    response = self.session.get(warc_url, stream=True, timeout=120)
                    for record in warcio.ArchiveIterator(response.raw):
                        if record.rec_type == 'response':
                            # Check content-type to ensure we only parse HTML
                            content_type = record.http_headers.get_header('Content-Type', '')
                            if 'text/html' not in content_type:
                                continue
                                
                            html_bytes = record.content_stream().read()
                            html_text = html_bytes.decode('utf-8', errors='ignore').lower()
                            
                            # Content-based heuristic filtering
                            if any(kw in html_text for kw in keywords):
                                url = record.rec_headers.get_header('WARC-Target-URI')
                                data = self.extract_vqa_data(html_bytes, url)
                                if data:
                                    f.write(json.dumps(data, ensure_ascii=False) + '\n')
                                    total_extracted += 1
                                    
                    # Polite delay between segments
                    time.sleep(random.uniform(2.0, 5.0))
                    
                except Exception as e:
                    logger.warning(f"Failed to process segment {warc_url}: {e}")
                    
        logger.info(f"Common Crawl acquisition complete. Extracted {total_extracted} disaster-relevant pairs.")

if __name__ == "__main__":
    scraper = MultimodalNewsScraper()
    scraper.execute_pipeline()