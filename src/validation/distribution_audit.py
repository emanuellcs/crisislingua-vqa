import json
import logging
from collections import Counter
from pathlib import Path
from typing import Dict

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - [%(levelname)s] - %(name)s - %(message)s'
)
logger = logging.getLogger("DistributionAuditor")

class DatasetAuditor:
    """
    Validates dataset balance to ensure compliance with the Uncharted Data Challenge 
    criteria for under-resourced languages.
    """
    
    # Our target languages from the architectural blueprint
    TARGET_LANGS = {"sw", "am", "tl", "ceb", "mr", "bho"}
    MINIMUM_REPRESENTATION_THRESHOLD = 0.05  # 5% minimum per target language
    
    def __init__(self, dataset_file: str = "../../data/processed/crisislingua_vqa_sanitized.jsonl"):
        self.dataset_file = Path(dataset_file)
        
    def analyze_distribution(self) -> Dict[str, float]:
        """Calculates the percentage distribution of languages in the dataset."""
        if not self.dataset_file.exists():
            raise FileNotFoundError(f"Dataset missing: {self.dataset_file}. Run PII Scrubber first.")
            
        lang_counts = Counter()
        total_records = 0
        
        logger.info(f"Scanning dataset for distribution metrics: {self.dataset_file}")
        
        with open(self.dataset_file, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip():
                    continue
                record = json.loads(line)
                lang = record.get("language_inferred", "unknown").lower()
                
                # Handle code-switched labels (e.g., "sw-en")
                primary_lang = lang.split('-')[0] 
                lang_counts[primary_lang] += 1
                total_records += 1
                
        if total_records == 0:
            logger.error("Dataset is empty.")
            return {}
            
        distribution = {k: (v / total_records) for k, v in lang_counts.items()}
        
        logger.info(f"Total valid records scanned: {total_records}")
        for lang, pct in distribution.items():
            logger.info(f"Language [{lang}]: {pct:.1%} ({lang_counts[lang]} records)")
            
        return distribution

    def validate_compliance(self, distribution: Dict[str, float]):
        """Asserts that all target languages meet the minimum threshold."""
        logger.info("--- Validating Challenge Compliance ---")
        
        passed = True
        for target in self.TARGET_LANGS:
            pct = distribution.get(target, 0.0)
            if pct < self.MINIMUM_REPRESENTATION_THRESHOLD:
                logger.warning(f"🚨 CRITICAL: Target language '{target}' is severely underrepresented ({pct:.1%}). "
                               f"Minimum threshold is {self.MINIMUM_REPRESENTATION_THRESHOLD:.1%}.")
                passed = False
            else:
                logger.info(f"✅ Language '{target}' meets thresholds.")
                
        if passed:
            logger.info("🎉 Dataset passes distribution audit. Ready for Kaggle deployment.")
        else:
            logger.warning("⚠️ Audit failed. Recommend adjusting heuristic keywords and re-running Phase 1.")

if __name__ == "__main__":
    auditor = DatasetAuditor()
    dist = auditor.analyze_distribution()
    if dist:
        auditor.validate_compliance(dist)