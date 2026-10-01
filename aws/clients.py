"""Boto3 client factory.

`config.AWS_ENDPOINT_URL` is set to LocalStack by default (see
.env.example) so everything here runs for free against a local container.
Unset it (or don't set AWS_ENDPOINT_URL) to point the exact same code at
real AWS.
"""
from __future__ import annotations

import boto3

from utils import config


def _client(service: str):
    kwargs = {"region_name": config.AWS_REGION}
    if config.AWS_ENDPOINT_URL:
        kwargs.update(
            endpoint_url=config.AWS_ENDPOINT_URL,
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
    return boto3.client(service, **kwargs)


def s3_client():
    return _client("s3")


def glue_client():
    return _client("glue")


def athena_client():
    return _client("athena")
