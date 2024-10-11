import torch
import os
from utility.S3Manager import S3Manager
from utility.constanst import *

s3_manager = S3Manager()

def set_device(verbose: bool = True) -> torch.device:
    """
    Set the device to the best available option: CUDA (if available), MPS (if available on Mac),
    or CPU as a fallback. Provides a robust selection mechanism for production environments.

    Args:
        verbose (bool): If True, prints out the selected device information. Default is True.

    Returns:
        torch.device: The best available device for computation.
    """
    # Check for available devices
    if torch.cuda.is_available():
        device = torch.device("cuda")
        device_name = torch.cuda.get_device_name(device)
        device_info = f"CUDA (GPU): {device_name}"
    elif torch.backends.mps.is_available() and torch.backends.mps.is_built():
        device = torch.device("mps")
        device_info = "MPS (Apple Silicon)"
    else:
        device = torch.device("cpu")
        device_info = "CPU"

    # Verbose output for debugging or tracking purposes
    if verbose:
        print(f"[INFO] Using device: {device_info}")

    return device


def download_model_if_needed(bucket_name, s3_prefix, local_path, force_download=False):
    """
    Download the model from S3 if it doesn't exist locally or if forced to download.

    Args:
    - bucket_name (str): Name of the S3 bucket.
    - s3_prefix (str): S3 folder prefix to download.
    - local_path (str): Local directory to store the model.
    - force_download (bool): If True, download even if the local folder exists.
    """
    if not os.path.isdir(local_path) or force_download:
        print(f"Downloading model from {s3_prefix} to {local_path}...")
        s3_manager.download_s3_folder(
            bucket_name=bucket_name,
            s3_prefix=s3_prefix,
            local_path=local_path
        )
        print(f"Downloaded model to {local_path}.")
    else:
        print(f"Model at {local_path} already exists. Skipping download.")


def download_ml_models(bucket_name, model_paths, force_download=False):
    """
    Downloads multiple machine learning models from an S3 bucket to the local system if they do not exist locally,
    or if the force download option is enabled.

    Args:
    - bucket_name (str): The name of the S3 bucket where the models are stored.
    - model_paths (list of tuples): A list of tuples where each tuple contains:
        - s3_prefix (str): The S3 folder prefix where the model is stored.
        - local_path (str): The local directory where the model should be downloaded.
    - force_download (bool, optional): If True, the models will be downloaded even if they already exist locally.
      Defaults to False.

    This function iterates through the list of models provided in `model_paths`, and for each model, it checks
    if the local directory exists. If it doesn't or if `force_download` is True, the model will be downloaded 
    from the S3 bucket to the specified local path.
    """

    for s3_prefix, local_path in model_paths:
        download_model_if_needed(bucket_name, s3_prefix, local_path, force_download)


