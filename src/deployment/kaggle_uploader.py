import os
import json
import logging
from pathlib import Path
import subprocess

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - [%(levelname)s] - %(name)s - %(message)s'
)
logger = logging.getLogger("KaggleDeployer")

class KaggleDeploymentPipeline:
    """
    Automates the creation of Kaggle metadata and pushes the final dataset
    to the Kaggle platform, ensuring strict compliance with hackathon licensing.
    """
    
    def __init__(self, dataset_dir: str = "../../data/processed"):
        self.dataset_dir = Path(dataset_dir)
        self.metadata_file = self.dataset_dir / "dataset-metadata.json"
        
        # Kaggle Dataset slug constraints: lowercase alphanumeric and hyphens
        self.dataset_slug = "crisislingua-vqa-adaption-hackathon"
        self.dataset_title = "CrisisLingua-VQA: Humanitarian Disaster Reasoning"
        
        # Ensure the Kaggle username is set via environment or default to a placeholder
        self.kaggle_username = os.getenv("KAGGLE_USERNAME", "your_kaggle_username")

    def generate_metadata(self):
        """
        Generates the dataset-metadata.json file required by the Kaggle API.
        Enforces the CC BY 4.0 license and the mandatory Adaption attribution.
        """
        logger.info("Generating Kaggle dataset metadata...")
        
        # The mandatory attribution string as per competition rules
        attribution = "This dataset was adapted and optimized using Adaptive Data by Adaption."
        
        metadata = {
            "title": self.dataset_title,
            "id": f"{self.kaggle_username}/{self.dataset_slug}",
            "licenses": [
                {
                    "name": "CC-BY-4.0" # Required by hackathon rules
                }
            ],
            "description": f"{self.dataset_title}\n\n{attribution}",
            "isPrivate": False # Must be open-source
        }
        
        with open(self.metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=4)
            
        logger.info(f"Metadata generated at {self.metadata_file}")

    def execute_upload(self):
        """Executes the Kaggle CLI command to push the dataset."""
        if not self.metadata_file.exists():
            raise FileNotFoundError("Metadata file missing. Cannot upload to Kaggle.")
            
        logger.info(f"Initiating upload to Kaggle for {self.kaggle_username}/{self.dataset_slug}...")
        
        try:
            # We use subprocess to call the kaggle CLI tool directly
            # 'kaggle datasets create -p <path>' creates a new dataset
            # To update an existing one, you would use 'kaggle datasets version'
            result = subprocess.run(
                ["kaggle", "datasets", "create", "-p", str(self.dataset_dir)],
                capture_output=True,
                text=True,
                check=True
            )
            logger.info("✅ Kaggle Upload Successful!")
            logger.info(result.stdout)
            
        except subprocess.CalledProcessError as e:
            logger.error("🚨 Kaggle Upload Failed.")
            logger.error(e.stderr)
            raise

if __name__ == "__main__":
    deployer = KaggleDeploymentPipeline()
    deployer.generate_metadata()
    deployer.execute_upload()