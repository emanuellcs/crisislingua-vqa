import sys
import logging
from pathlib import Path

# Add src to the path to resolve imports
sys.path.append(str(Path(__file__).resolve().parent))

from src.acquisition.twb_glossary import TWBGlossaryIngestor
from src.acquisition.ushahidi_api import UshahidiArchiveFetcher
from src.acquisition.multimodal_scraper import MultimodalNewsScraper
from src.adaptation.schema_mapper import AdaptationPipeline
from src.validation.pii_scrubber import DataScrubber
from src.validation.distribution_audit import DatasetAuditor
from src.deployment.kaggle_uploader import KaggleDeploymentPipeline

# Configure global root logger
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - [%(levelname)s] - %(name)s - %(message)s'
)
logger = logging.getLogger("HackathonOrchestrator")

def run_phase_1():
    logger.info("=== STARTING PHASE 1: ACQUISITION ===")
    
    # 1. Ingest TWB Glossary
    twb = TWBGlossaryIngestor(output_dir="data/intermediate")
    twb.execute_pipeline()
    
    # 2. Fetch Ushahidi Archives
    ushahidi = UshahidiArchiveFetcher(output_dir="data/raw")
    ushahidi.execute_pipeline()
    
    # 3. Multimodal Web Scrape
    scraper = MultimodalNewsScraper(
        heuristics_path="data/intermediate/twb_heuristic_keywords.json", 
        output_dir="data/raw"
    )
    scraper.execute_pipeline()
    logger.info("=== PHASE 1 COMPLETE ===")

def run_phase_2():
    logger.info("=== STARTING PHASE 2: ADAPTATION ===")
    
    pipeline = AdaptationPipeline(
        raw_data_paths=[
            "data/raw/ushahidi_raw_reports.jsonl", 
            "data/raw/multimodal_news_scrape.jsonl"
        ],
        output_dir="data/intermediate"
    )
    pipeline.process_pipeline()
    logger.info("=== PHASE 2 COMPLETE ===")

def run_phase_3() -> bool:
    logger.info("=== STARTING PHASE 3: VALIDATION ===")
    
    # 1. PII Scrubbing
    scrubber = DataScrubber(
        input_file="data/intermediate/reshaped_mapped_reports.jsonl", 
        output_dir="data/processed"
    )
    scrubber.execute()
    
    # 2. Distribution Audit
    auditor = DatasetAuditor(dataset_file="data/processed/crisislingua_vqa_sanitized.jsonl")
    distribution = auditor.analyze_distribution()
    
    # Validate compliance with Uncharted Data Challenge rules
    if not distribution:
        logger.error("Audit failed. Dataset is empty.")
        return False
        
    passed = True
    for target in auditor.TARGET_LANGS:
        pct = distribution.get(target, 0.0)
        if pct < auditor.MINIMUM_REPRESENTATION_THRESHOLD:
            passed = False
            
    if passed:
        logger.info("=== PHASE 3 COMPLETE. DATASET IS COMPLIANT. ===")
        return True
    else:
        logger.error("=== PHASE 3 FAILED. PIPELINE HALTED. ===")
        logger.error("Adjust TWB heuristics or scraping parameters before deploying.")
        return False

def run_phase_4():
    logger.info("=== STARTING PHASE 4: DEPLOYMENT ===")
    
    deployer = KaggleDeploymentPipeline(dataset_dir="data/processed")
    deployer.generate_metadata()
    deployer.execute_upload()
    logger.info("=== PHASE 4 COMPLETE. DATASET IS LIVE ON KAGGLE. ===")

def main():
    logger.info("🚀 Initiating CrisisLingua-VQA Hackathon Pipeline 🚀")
    
    try:
        run_phase_1()
        run_phase_2()
        
        # Only deploy if the validation audit passes
        is_compliant = run_phase_3()
        if is_compliant:
            run_phase_4()
            logger.info("🏆 Hackathon pipeline executed successfully. Ready for submission.")
        else:
            logger.warning("Deployment aborted due to failed validation constraints.")
            sys.exit(1)
            
    except Exception as e:
        logger.critical(f"Pipeline crashed with unhandled exception: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
