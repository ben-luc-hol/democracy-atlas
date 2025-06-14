from google.cloud import storage
from jedi import settings

from google.cloud import storage
from google.cloud.exceptions import NotFound
import json
from pathlib import Path
from typing import Dict, List, Optional, Union
import logging


class AtlasCloudManager:

    def __init__(self, project_id: str = "democracy-atlas", bucket_name: str = "democracy-atlas"):
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.client = storage.Client(project=project_id)
        self.bucket = self.client.bucket(bucket_name)

    def upload_json(self, data: Dict, blob_path: str) -> str:
        """Upload JSON data to specified path"""
        pass

    def download_json(self, blob_path: str) -> Dict:
        """Download and parse JSON from specified path"""
        pass

    def build_path(self, data_type: str, country: str, **kwargs) -> str:
        """Build standardized cloud storage paths"""
        pass

    def list_files(self, prefix: str) -> List[str]:
        """List files with given prefix"""
        pass

    def batch_upload(self, files: Dict[str, Dict]) -> List[str]:
        """Upload multiple files efficiently"""
        pass