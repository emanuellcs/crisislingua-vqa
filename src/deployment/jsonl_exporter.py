import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Generator

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(name)s - %(message)s"
)
logger = logging.getLogger("JSONLExporter")


class JSONLExporter:
    """
    Production exporter ensuring the final dataset artifacts are strictly
    formatted as highly interoperable JSON Lines (JSONL) files.
    """

    def __init__(
        self, output_file: str = "../../data/processed/crisislingua_vqa_final.jsonl"
    ):
        self.output_file = Path(output_file)
        self.output_file.parent.mkdir(parents=True, exist_ok=True)

    def export_batch(self, records: List[Dict[str, Any]], append: bool = True):
        """
        Writes a batch of fully processed, sanitized records to the final JSONL artifact.
        """
        mode = "a" if append else "w"

        with open(self.output_file, mode, encoding="utf-8") as f:
            for record in records:
                # Enforce ASCII to False to preserve under-resourced language characters
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

        logger.debug(f"Exported {len(records)} records to {self.output_file}")

    def export_stream(
        self, record_stream: Generator[Dict[str, Any], None, None], append: bool = False
    ):
        """
        Consumes a generator and writes directly to disk for memory-safe final deployment.
        """
        mode = "a" if append else "w"
        total_exported = 0

        logger.info(f"Starting final export stream to {self.output_file}")

        with open(self.output_file, mode, encoding="utf-8") as f:
            for record in record_stream:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
                total_exported += 1

        logger.info(
            f"Final export complete. {total_exported} records written to {self.output_file}"
        )


if __name__ == "__main__":
    # Example usage for final validation check
    exporter = JSONLExporter()
    logger.info(f"Exporter initialized. Targeting: {exporter.output_file}")
