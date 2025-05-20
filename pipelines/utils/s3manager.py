import os
import boto3
import pandas as pd
from io import BytesIO
import json


class S3Manager:
    def __init__(self, bucket: str = None, aws_profile: str = None, region: str = None):
        """
        bucket: S3 bucket name
        aws_profile: name of AWS CLI profile (optional)
        region: AWS region (optional, defaults to env or boto3 default)
        """
        self.bucket = bucket or os.getenv("ATLAS_S3_BUCKET")
        session_args = {}
        if aws_profile:
            session_args["profile_name"] = aws_profile
        self.session = boto3.Session(**session_args)
        self.s3 = self.session.resource("s3", region_name=region)
        self.client = self.session.client("s3", region_name=region)

    def upload_file(self, local_path: str, key: str, **extra_args):
        """Upload a local file to s3://bucket/key"""
        self.client.upload_file(local_path, self.bucket, key, ExtraArgs=extra_args)

    def download_file(self, key: str, local_path: str):
        """Download s3://bucket/key to local_path"""
        self.client.download_file(self.bucket, key, local_path)

    def list_keys(self, prefix: str):
        """List all object keys under a given prefix"""
        bucket = self.s3.Bucket(self.bucket)
        return [obj.key for obj in bucket.objects.filter(Prefix=prefix)]

    def delete_keys(self, keys: list[str]):
        """Bulk delete a list of keys"""
        if not keys:
            return
        objs = [{"Key": k} for k in keys]
        self.client.delete_objects(Bucket=self.bucket, Delete={"Objects": objs})

    def read_parquet(self, key: str) -> pd.DataFrame:
        """Read a parquet file from S3 into a DataFrame"""
        uri = f"s3://{self.bucket}/{key}"
        # uses s3fs under the hood
        return pd.read_parquet(uri)

    def write_parquet(self, df: pd.DataFrame, key: str, **parquet_kwargs):
        """Write a DataFrame to S3 as parquet"""
        uri = f"s3://{self.bucket}/{key}"
        df.to_parquet(uri, **parquet_kwargs)

    def read_csv(self, key: str, **read_csv_kwargs) -> pd.DataFrame:
        """Read a CSV from S3 into a DataFrame"""
        uri = f"s3://{self.bucket}/{key}"
        return pd.read_csv(uri, **read_csv_kwargs)

    def write_csv(self, df: pd.DataFrame, key: str, **to_csv_kwargs):
        """Write a DataFrame to S3 as CSV"""
        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index=False, **to_csv_kwargs)
        self.client.put_object(Bucket=self.bucket, Key=key, Body=csv_buffer.getvalue())

    def read_json(self, key: str) -> dict:
        """Read a JSON file from S3"""
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)

    def write_json(self, data: dict | list, key: str, indent: int = None):
        """Write data as JSON to S3

        Args:
            data: Python dict or list to serialize to JSON
            key: S3 object key
            indent: Optional JSON indentation for pretty printing
        """
        json_buffer = BytesIO()
        json_buffer.write(json.dumps(data, indent=indent).encode('utf-8'))
        json_buffer.seek(0)
        self.client.put_object(Bucket=self.bucket, Key=key, Body=json_buffer.getvalue())

    def read_geojson(self, key: str) -> dict:
        """Read a GeoJSON file from S3"""
        return self.read_json(key)

    def write_geojson(self, geojson_data: dict, key: str, indent: int = None):
        """Write GeoJSON data to S3

        Args:
            geojson_data: GeoJSON data as Python dict
            key: S3 object key
            indent: Optional JSON indentation for pretty printing
        """
        self.write_json(geojson_data, key, indent=indent)

    def generate_presigned_url(self, key: str, expires_in: int = 3600) -> str:
        """Get a presigned GET URL for the object"""
        return self.client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket, "Key": key},
            ExpiresIn=expires_in
        )

