import re
import json
import logging
import sys
from pathlib import Path
from typing import Dict, Any, Generator

# Add root src to path so we can import the deployment exporter
sys.path.append(str(Path(__file__).resolve().parents[2]))
from deployment.jsonl_exporter import JSONLExporter

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - [%(levelname)s] - %(name)s - %(message)s'
)
logger = logging.getLogger("PIIScrubber")

class DataScrubber:
    """
    Production-grade PII scrubber. 
    Streams data, redacts sensitive information, and routes to JSONLExporter.
    """
    
    PII_PATTERNS = {
        "EMAIL": r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+",
        "PHONE": r"(\+?\d{1,3}[-.\s]?)?(\(?\d{2,4}\)?[-.\s]?)?\d{3,4}[-.\s]?\d{4}",
        "IP_ADDRESS": r"\b(?:\d{1,3}\.){3}\d{1,3}\b",
        "CREDIT_CARD": r"\b(?:\d[ -]*?){13,16}\b"
    }

    def __init__(self, input_file: str = "../../data/intermediate/reshaped_mapped_reports.jsonl", 
                 output_dir: str = "../../data/processed"):
        self.input_file = Path(input_file)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.output_file = self.output_dir / "crisislingua_vqa_sanitized.jsonl"
        
        # Initialize the shared JSONL exporter
        self.exporter = JSONLExporter(output_file=str(self.output_file))
        
        self.compiled_patterns = {
            key: re.compile(pattern) for key, pattern in self.PII_PATTERNS.items()
        }

    def redact_text(self, text: str) -> str:
        """Applies regex substitutions to redact PII from a single string."""
        if not text:
            return text
            
        redacted_text = text
        for pii_type, pattern in self.compiled_patterns.items():
            redacted_text = pattern.sub(f"[REDACTED_{pii_type}]", redacted_text)
            
        return redacted_text

    def stream_and_scrub(self) -> Generator[Dict[str, Any], None, None]:
        """Memory-efficient generator that yields scrubbed JSON objects."""
        if not self.input_file.exists():
            raise FileNotFoundError(f"Input file not found: {self.input_file}. Run Phase 2 first.")
            
        logger.info(f"Initiating PII scrubbing on {self.input_file}")
        
        with open(self.input_file, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip():
                    continue
                    
                record = json.loads(line)
                
                if "content" in record:
                    record["content"] = self.redact_text(record["content"])
                if "title" in record:
                    record["title"] = self.redact_text(record["title"])
                    
                yield record

    def execute(self):
        """Executes the scrubbing pipeline and streams output directly to the exporter."""
        logger.info("Executing PII Scrubber stream...")
        self.exporter.export_stream(self.stream_and_scrub(), append=False)
        logger.info("PII Scrubbing and export routing complete.")

if __name__ == "__main__":
    scrubber = DataScrubber()
    scrubber.execute()