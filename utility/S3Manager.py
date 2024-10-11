
import boto3
import botocore
import os
from typing import List, Optional

class S3Manager:
    def __init__(self) -> None:
        """
        Initializes the S3Manager with an S3 client.
        """
        self.s3 = boto3.client("s3")

    def list_buckets(self) -> Optional[List[str]]:
        """
        Lists all the S3 buckets.

        Returns:
            Optional[List[str]]: A list of bucket names or None if an error occurs.
        """
        try:
            response = self.s3.list_buckets()
            buckets = [bucket['Name'] for bucket in response['Buckets']]
            return buckets
        except botocore.exceptions.ClientError as e:
            print(f"Error listing buckets: {e}")
            return None

    def create_bucket(self, bucket_name: str) -> None:
        """
        Creates an S3 bucket.

        Args:
            bucket_name (str): The name of the S3 bucket to create.
        """
        try:
            all_buckets = self.list_buckets()
            if bucket_name not in all_buckets:
                self.s3.create_bucket(Bucket=bucket_name)
                print(f"S3 Bucket with name '{bucket_name}' is created !!!")
            else:
                print(f"Bucket '{bucket_name}' already exists.")

        except botocore.exceptions.ClientError as e:
                print(f"Error creating bucket: {e}")

    def upload_file(self, file_path: str, bucket_name: str, object_name: Optional[str] = None) -> None:
        """
        Uploads a file to the specified S3 bucket.

        Args:
            file_path (str): The path to the file to upload.
            bucket_name (str): The name of the S3 bucket.
            object_name (Optional[str]): The object name in the S3 bucket (defaults to the file name).
        """
        if object_name is None:
            object_name = os.path.basename(file_path)

        try:
            self.s3.upload_file(Filename=file_path, Bucket=bucket_name, Key=object_name)
            print(f"File '{file_path}' uploaded to S3 as '{object_name}'")
        except botocore.exceptions.ClientError as e:
            print(f"Error uploading file: {e}")

    def list_objects_in_bucket(self, bucket_name: str) -> Optional[List[str]]:
        """
        Lists all objects in the specified S3 bucket.

        Args:
            bucket_name (str): The name of the S3 bucket.

        Returns:
            Optional[List[str]]: A list of object keys or None if an error occurs.
        """
        try:
            response = self.s3.list_objects_v2(Bucket=bucket_name)
            object_keys = [obj['Key'] for obj in response.get('Contents', [])]
            return object_keys
        except botocore.exceptions.ClientError as e:
            print(f"Error listing objects in bucket: {e}")
            return None

    def download_file(self, object_name: str, bucket_name: str, file_path: str) -> None:
        """
        Downloads a file from an S3 bucket to a local path.

        Args:
            object_name (str): The key of the object in the S3 bucket.
            bucket_name (str): The name of the S3 bucket.
            file_path (str): The local path where the object will be downloaded.
        """
        if not os.path.exists(os.path.dirname(file_path)):
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            print(f"{file_path} directory created !!!")

        try:
            self.s3.download_file(Bucket=bucket_name, Key=object_name, Filename=file_path)
            print(f"Downloaded '{object_name}' from S3 bucket '{bucket_name}' to '{file_path}'")
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Message'] == 'Not Found':
                print(f"Object '{object_name}' not found in bucket '{bucket_name}'")
            else:
                print(f"Error downloading file: {e}")

    def upload_folder(self, directory_path: str, bucket_name: str, s3_prefix: str) -> None:
        """
        Uploads a folder and its contents to an S3 bucket.

        Args:
            directory_path (str): The local directory path.
            bucket_name (str): The name of the S3 bucket.
            s3_prefix (str): The folder prefix in the S3 bucket where files will be uploaded.
        """
        try:
            for root, dirs, files in os.walk(directory_path):
                for file in files:
                    file_path = os.path.join(root, file).replace("\\", "/")
                    s3_key = os.path.join(s3_prefix, file).replace("\\", "/")
                    self.s3.upload_file(Filename=file_path, Bucket=bucket_name, Key=s3_key)
                    print(f"Uploaded '{file_path}' to S3 as '{s3_key}'")
        except botocore.exceptions.ClientError as e:
            print(f"Error uploading folder: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    def download_s3_folder(self, bucket_name: str, s3_prefix: str, local_path: str) -> None:
        """
        Downloads an S3 folder and its contents to a local directory.

        Args:
            bucket_name (str): The name of the S3 bucket.
            s3_prefix (str): The folder prefix in the S3 bucket.
            local_path (str): The local directory path to store the downloaded folder.
        """
        os.makedirs(local_path, exist_ok=True)
        paginator = self.s3.get_paginator("list_objects_v2")

        try:
            for page in paginator.paginate(Bucket=bucket_name, Prefix=s3_prefix):
                if "Contents" in page:
                    for obj in page["Contents"]:
                        s3_key = obj["Key"]
                        local_file_path = os.path.join(local_path, os.path.basename(s3_key))
                        self.s3.download_file(Bucket=bucket_name, Key=s3_key, Filename=local_file_path)
                        print(f"Downloaded '{s3_key}' to '{local_file_path}'")
        except botocore.exceptions.ClientError as e:
            print(f"Error downloading S3 folder: {e}")

    def delete_objects(self, bucket_name: str) -> None:
        """
        Deletes all objects in the specified S3 bucket.

        Args:
            bucket_name (str): The name of the S3 bucket.
        """
        paginator = self.s3.get_paginator("list_objects_v2")
        try:
            for page in paginator.paginate(Bucket=bucket_name):
                if "Contents" in page:
                    objects_to_delete = [{"Key": obj["Key"]} for obj in page["Contents"]]
                    response = self.s3.delete_objects(Bucket=bucket_name, Delete={"Objects": objects_to_delete})

                    if "Errors" in response:
                        for error in response["Errors"]:
                            print(f"Error deleting object: {error}")
                    else:
                        print(f"Deleted {len(objects_to_delete)} objects from bucket '{bucket_name}'")
        except botocore.exceptions.ClientError as e:
            print(f"Error deleting objects: {e}")

    def delete_bucket(self, bucket_name: str) -> None:
        """
        Deletes the specified S3 bucket.

        Args:
            bucket_name (str): The name of the S3 bucket to delete.
        """
        try:
            all_buckets = self.list_buckets()
            if bucket_name in all_buckets:
                self.s3.delete_bucket(Bucket=bucket_name)
                print(f"Bucket '{bucket_name}' deleted successfully.")
            else:
                print(f"Bucket '{bucket_name}' not found on S3")
        except botocore.exceptions.ClientError as e:
            print(f"Error deleting bucket: {e}")
