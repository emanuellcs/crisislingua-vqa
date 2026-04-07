import os
import json
import logging
from pathlib import Path
from typing import Dict, List, Any
import requests
from dotenv import load_dotenv
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

load_dotenv()

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - [%(levelname)s] - %(name)s - %(message)s'
)
logger = logging.getLogger("TWB_Ingestor")

class GlossaryAcquisitionError(Exception):
    """Custom exception for glossary extraction failures."""
    pass

class TWBGlossaryIngestor:
    """
    Production client for extracting structured disaster terminology 
    from authenticated humanitarian term bases.
    """
    
    # Target ISO 639-1 / 639-3 codes based on our architecture
    TARGET_LANGUAGES = ["sw", "am", "tl", "ceb", "mr", "bho"]
    
    def __init__(self, output_dir: str = "../../data/intermediate"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.output_file = self.output_dir / "twb_heuristic_keywords.json"
        
        self.api_key = os.getenv("TWB_API_KEY")
        self.endpoint = os.getenv("TWB_GLOSSARY_ENDPOINT")
        
        if not self.api_key or not self.endpoint:
            raise ValueError("Missing TWB_API_KEY or TWB_GLOSSARY_ENDPOINT in environment variables.")

    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=10),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(requests.exceptions.RequestException)
    )
    def fetch_terminology_base(self) -> Dict[str, Any]:
        """
        Executes an authenticated request to the term base API.
        Protected by exponential backoff (retries up to 5 times, waiting 2-10 seconds).
        """
        logger.info(f"Querying term base endpoint: {self.endpoint}")
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json"
        }
        
        response = requests.get(self.endpoint, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()

    def process_and_filter(self, raw_data: Dict[str, Any]) -> Dict[str, List[str]]:
        """
        Filters the massive global term base down to our specific target 
        languages and crisis categories.
        """
        logger.info("Filtering raw terminology base...")
        processed_heuristics = {lang: [] for lang in self.TARGET_LANGUAGES}
        
        # Assumption: The API returns a list of term objects: 
        # {"term": "mafuriko", "language": "sw", "category": "disaster"}
        terms = raw_data.get("terms", [])
        
        for item in terms:
            lang = item.get("language")
            category = item.get("category", "").lower()
            term = item.get("term")
            
            if lang in self.TARGET_LANGUAGES and category in ["disaster", "crisis", "infrastructure"]:
                if term and term not in processed_heuristics[lang]:
                    processed_heuristics[lang].append(term)
                    
        return processed_heuristics

    def execute_pipeline(self):
        """Runs the fetch, process, and save workflow."""
        try:
            raw_data = self.fetch_terminology_base()
            filtered_keywords = self.process_and_filter(raw_data)
            
            final_payload = {
                "metadata": {
                    "source": self.endpoint,
                    "target_languages": self.TARGET_LANGUAGES,
                    "purpose": "Heuristic Keyword Matching for Scraper"
                },
                "keywords": filtered_keywords
            }
            
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(final_payload, f, ensure_ascii=False, indent=4)
                
            logger.info(f"Successfully saved {sum(len(v) for v in filtered_keywords.values())} heuristics to {self.output_file}")
            
        except Exception as e:
            logger.error(f"Failed to execute glossary pipeline: {e}")
            raise

if __name__ == "__main__":
    ingestor = TWBGlossaryIngestor()
    ingestor.execute_pipeline()