import os
import json
import logging
from pathlib import Path
from typing import Dict, List, Any
import requests
from dotenv import load_dotenv
from tenacity import (
    retry,
    wait_exponential,
    stop_after_attempt,
    retry_if_exception_type,
)
from bs4 import BeautifulSoup

load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(name)s - %(message)s"
)
logger = logging.getLogger("TWB_Ingestor")


class GlossaryAcquisitionError(Exception):
    """Custom exception for glossary extraction failures."""

    pass


class TWBGlossaryIngestor:
    """
    Production client for extracting structured disaster terminology
    from CLEAR Global's public Language Use Data Platform (LUDP) and
    web scraping of their terminology glossaries.
    """

    # Target ISO 639-3 codes for LUDP and regional filtering
    TARGET_LANG_CODES = ["swa", "amh", "tgl", "ceb", "mar", "bho"]
    LOCATION_CODES = ["KEN", "PHL", "IND"]

    FALLBACK_KEYWORDS = {
        "tgl": ["lindol", "bagyo", "baha", "sakuna", "tulong", "sunog", "paglikas"],
        "ceb": ["linog", "bagyo", "baha", "disastre", "tabang", "sunog", "bakwit"],
    }

    def __init__(self, output_dir: str = "data/intermediate"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.output_file = self.output_dir / "twb_heuristic_keywords.json"

        self.ludp_base_url = "https://ludp.clearglobal.org/public/location/"
        self.glossary_base_url = "https://glossaries.clearglobal.org/"

    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=10),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(requests.exceptions.RequestException),
    )
    def fetch_ludp_data(self, location_code: str) -> List[Dict[str, Any]]:
        """
        Fetches language data from the public LUDP endpoint.
        Returns a list of language usage and terminology records.
        """
        url = f"{self.ludp_base_url}{location_code}"
        params = {"page": 0, "page_size": 500}
        logger.info(f"Querying LUDP for location: {location_code}")

        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        # The LUDP response contains results in a 'results' key or similar structure
        data = response.json()
        return data.get("results", data.get("data", []))

    def scrape_glossary_site(self, category: str) -> Dict[str, List[str]]:
        """
        Scrapes static HTML glossaries from Clear Global for localized
        disaster terminology.
        """
        url = f"{self.glossary_base_url}{category}/"
        logger.info(f"Scraping terminology from glossary: {url}")
        scraped_data = {lang: [] for lang in self.TARGET_LANG_CODES}

        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            # Target table rows or definition lists containing localized terms
            for row in soup.find_all(["tr", "div", "li"]):
                text = row.get_text().lower()
                # Heuristic: if a row mentions a target language, extract the term
                for lang in self.TARGET_LANG_CODES:
                    if lang in text:
                        # Extract the first prominent word as the term
                        words = [w.strip() for w in text.split() if len(w.strip()) > 3]
                        if words:
                            scraped_data[lang].append(words[0])
        except Exception as e:
            logger.warning(f"Glossary scraping for {category} failed: {e}")

        return scraped_data

    def execute_pipeline(self):
        """Runs the multi-source acquisition pipeline."""
        logger.info("Starting TWB glossary acquisition pipeline...")
        combined_keywords = {lang: [] for lang in self.TARGET_LANG_CODES}

        # 1. Harvest from LUDP
        for loc in self.LOCATION_CODES:
            try:
                records = self.fetch_ludp_data(loc)
                for rec in records:
                    lang = rec.get("language_code")
                    term = rec.get("term") or rec.get("language_name_local")
                    if lang in self.TARGET_LANG_CODES and term:
                        if term not in combined_keywords[lang]:
                            combined_keywords[lang].append(term)
            except Exception as e:
                logger.error(f"Failed to fetch LUDP data for {loc}: {e}")

        # 2. Harvest from Web Scraping
        for category in [
            "covid19",
            "wfp",
            "bangladesh",
            "myanmar",
            "drc",
            "psea",
            "psea_sa",
            "oxfam",
            "iom",
            "mozambique",
            "refcrisis",
            "tigray",
        ]:
            scraped = self.scrape_glossary_site(category)
            for lang, terms in scraped.items():
                for t in terms:
                    if t not in combined_keywords[lang]:
                        combined_keywords[lang].append(t)

        # 3. Apply Fallbacks for dropped connections (PHL)
        for lang, fallbacks in self.FALLBACK_KEYWORDS.items():
            if not combined_keywords.get(lang):
                logger.info(f"Injecting fallback keywords for {lang}")
                combined_keywords[lang] = fallbacks

        final_payload = {
            "metadata": {
                "source": "LUDP + Clear Global Web Scraping",
                "target_languages": self.TARGET_LANG_CODES,
                "timestamp": json.dumps(True),  # Just a placeholder for valid JSON
            },
            "keywords": combined_keywords,
        }

        with open(self.output_file, "w", encoding="utf-8") as f:
            json.dump(final_payload, f, ensure_ascii=False, indent=4)

        logger.info(f"TWB acquisition complete. Saved to {self.output_file}")


if __name__ == "__main__":
    ingestor = TWBGlossaryIngestor()
    ingestor.execute_pipeline()
