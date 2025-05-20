import requests
import json
from datetime import datetime as dt
from .results_processor import ResultsPipeline
from .pipelines.utils.s3manager import S3Manager


class NorwayPipeline(ResultsPipeline):
    """
    Class to get and process historical Norwegian election data from
    official source ("valgdirektoratet" - Election directorate)
    Inherits from ResultsPipeline superclass
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.country = "nor"
        self.year = kwargs.get("year")
        self.s3 = S3Manager(bucket="election-atlas")

    def get_subdivisions(self):



