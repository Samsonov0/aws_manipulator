from boto3.s3.transfer import TransferConfig


class Settings:
    """
    На место значений в S3_CONTROL_SETTINGS необходимо поставить нужные данные
    """

    GB = 1024**3
    config = TransferConfig(multipart_threshold=5 * GB)

    S3_CONTROL_SETTINGS = {
        "bucket_name": "Bucker Name",
        "service_name": "s3",
        "endpoint_url": "Storage URL",
        "region_name": "Region Name",
        "aws_access_key_id": "AWS Access Key",
        "aws_secret_access_key": "AWS Secret Access Key",
    }
