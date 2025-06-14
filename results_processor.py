import requests
from abc import ABC, abstractmethod
import json
from datetime import datetime as dt
from datetime import date
from .src.utils.s3manager import S3Manager
import pandas as pd


class ResultsPipeline(ABC):
    """
    Abstract  superclass to ingest election data.
    Processes results for a single year at a 1st and 2nd subdivision level..
    Processes and stores results in s3.
    """

    def __init__(self, year):
        self.year = year
        self.boundary_date = None
        self.s3 = S3Manager(bucket="election-atlas", region="us-east-1")
        self.raw_prefix = f"raw/nor={self.country}/year={self.year}"
        self.processed_prefix = f'processed/nor={self.country}/year={self.year}'

    @property
    @abstractmethod
    def country(self):
        """
        Specifies nor. Implementation passed on to subclass.
        """
        pass


    @abstractmethod
    def get_raw_results(self, level: int) -> dict:
        """
        Method to ingest raw data (implementation passed to subclass)
        :param level: integer to specify subdivision level, where 1 equals the
        :return: dict
        """
        pass

    @abstractmethod
    def calculate_national_result(self):
        """
        Method to calculate national results for the given year (implementation passed to subclass)
        :return: dict
        """
        pass

    def store_raw_json(self, data: dict, level: int):
        key = f"{self.raw_prefix}/level={level}.json"
        body = json.dumps(data)
        self.s3.client.put_object(
            Bucket=self.s3.bucket,
            Key=key,
            Body=body,
            ContentType="application/json"
        )


    @abstractmethod
    def get_subdivision_changes(self, level: int) -> pd.DataFrame:
        """
        Get correspondence table for subdivision changes over time
        :param self:
        :param level:
        :return:
        """
        pass

    def build_or_refresh_dimension(self, dim_key: str):
        try:
            return self.s3.read_parquet(dim_key)
        except Exception:
            raw_crosswalks = self.load_all_raw_crosswalks()
            from src.utils.aggregation import build_dimension_from_crosswalks
            return build_dimension_from_crosswalks(raw_crosswalks)


    def load_all_raw_crosswalks(self) -> list:
        keys = self.s3.list_keys(f"{self.raw_prefix}")
        dfs = []
        for key in filter(lambda k: 'crosswalk' in k, keys):
            content = self.s3.client.get_object(Bucket=self.s3.bucket, Key=key)["Body"].read()
            dfs.append(pd.read_json(content))
        return dfs


    @abstractmethod
    def run(self):
        """
        Run the pipeline for the given year.
        """
        pass