import os
import logging
from typing import List, Dict, Any
import requests
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("AdaptiveClient")

class AdaptiveDataError(Exception):
    """Custom exception for Adaptive Data platform failures."""
    pass

class AdaptiveDataClient:
    """
    Production SDK wrapper for the Adaptive Data platform.
    Executes dynamic reshaping, intent extraction, and schema mapping via API.
    """
    
    def __init__(self):
        self.api_key = os.getenv("ADAPTIVE_DATA_API_KEY")
        self.base_url = os.getenv("ADAPTIVE_DATA_ENDPOINT", "https://api.adaptionlabs.ai/v1")
        
        if not self.api_key:
            raise ValueError("ADAPTIVE_DATA_API_KEY is missing from environment variables.")
            
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "X-Workload-Strategy": "dynamic-fusion" # Enables operation reordering
        })

    @retry(
        wait=wait_exponential(multiplier=1.5, min=2, max=15),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(requests.exceptions.RequestException)
    )
    def reshape_batch(self, batch: List[Dict[str, Any]], operation: str) -> List[Dict[str, Any]]:
        """
        Sends a batch of raw records to the Adaptive Data platform to execute 
        a specific reshaping operation (e.g., 'filter_noise', 'extract_intent', 'map_fema_esf').
        """
        endpoint = f"{self.base_url}/reshape/{operation}"
        
        payload = {
            "records": batch,
            "context_parameters": {
                "strict_schema_enforcement": True,
                "multilingual_projection": True
            }
        }
        
        logger.info(f"Sending batch of {len(batch)} records for operation: '{operation}'")
        
        response = self.session.post(endpoint, json=payload, timeout=30)
        response.raise_for_status()
        
        result_data = response.json()
        return result_data.get("reshaped_records", [])