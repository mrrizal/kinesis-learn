import boto3
from settings import AWS_ACCESS_KEY_ID, AWS_SECRET_KEY, AWS_DEFAULT_REGION


def get_client(service_name):
    client = boto3.client(service_name,
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_KEY,
                          region_name=AWS_DEFAULT_REGION)
    return client
