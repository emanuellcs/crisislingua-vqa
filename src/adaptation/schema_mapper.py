import json
import logging
from pathlib import Path
from typing import Generator, List, Dict, Any
from adaptive_client import AdaptiveDataClient

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - [%(levelname)s] - %(name)s - %(message)s'
)
logger = logging.getLogger("SchemaMapper")

class AdaptationPipeline:
    """
    Streams raw data through the Adaptive Data platform to sanitize, 
    extract intent, and map to FEMA ESF humanitarian frameworks.
    """
    
    def __init__(self, raw_data_paths: List[str], output_dir: str = "../../data/intermediate"):
        self.raw_data_paths = [Path(p) for p in raw_data_paths]
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.output_file = self.output_dir / "reshaped_mapped_reports.jsonl"
        
        self.client = AdaptiveDataClient()
        self.batch_size = 50  # Optimized for NLP/VLM payload limits

    def stream_raw_data(self) -> Generator[Dict[str, Any], None, None]:
        """Yields records from raw JSONL files one by one to save memory."""
        for file_path in self.raw_data_paths:
            if not file_path.exists():
                logger.warning(f"Input file missing: {file_path}. Skipping.")
                continue
                
            logger.info(f"Reading raw stream from {file_path}")
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        yield json.loads(line)

    def process_pipeline(self):
        """
        Executes the three-step Adaptation process:
        1. Noise Filtering
        2. Intent Extraction (Code-switched text)
        3. FEMA ESF Schema Mapping
        """
        current_batch = []
        total_processed = 0
        
        with open(self.output_file, 'w', encoding='utf-8') as out_f:
            for record in self.stream_raw_data():
                current_batch.append(record)
                
                if len(current_batch) >= self.batch_size:
                    self._execute_batch_and_write(current_batch, out_f)
                    total_processed += len(current_batch)
                    current_batch = []
            
            # Process any remaining records in the final partial batch
            if current_batch:
                self._execute_batch_and_write(current_batch, out_f)
                total_processed += len(current_batch)
                
        logger.info(f"Adaptation Pipeline complete. Successfully reshaped {total_processed} records.")

    def _execute_batch_and_write(self, batch: List[Dict[str, Any]], file_obj):
        """Passes the batch sequentially through the platform's reshaping endpoints."""
        try:
            # Step 1: Strip irrelevant social media chatter
            filtered_batch = self.client.reshape_batch(batch, operation="filter_noise")
            
            # Step 2: Reshape code-switched phrases into formalized intents
            intent_batch = self.client.reshape_batch(filtered_batch, operation="extract_intent")
            
            # Step 3: Map the text to standard humanitarian frameworks (FEMA ESF)
            mapped_batch = self.client.reshape_batch(intent_batch, operation="map_fema_esf")
            
            # Write optimized data to disk
            for record in mapped_batch:
                file_obj.write(json.dumps(record, ensure_ascii=False) + '\n')
                
        except Exception as e:
            logger.error(f"Batch processing failed. Dropping batch to preserve pipeline stability: {e}")

if __name__ == "__main__":
    # Point the pipeline to the outputs from the data collection phase
    input_files = [
        "../../data/raw/ushahidi_raw_reports.jsonl",
        "../../data/raw/multimodal_news_scrape.jsonl"
    ]
    
    pipeline = AdaptationPipeline(raw_data_paths=input_files)
    pipeline.process_pipeline()