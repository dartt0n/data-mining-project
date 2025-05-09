# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "click>=8.1.8",
#     "boto3>=1.35.99",
#     "botocore>=1.35.0,<1.36",
# ]
# ///


from pathlib import Path
import click
from boto3.session import Session as BotoSession


@click.command()
@click.argument("file")
@click.option("--s3-endpoint")
@click.option("--s3-bucket")
@click.option("--s3-access-key")
@click.option("--s3-secret-key")
@click.option("--s3-region")
def main(
    file: str,
    s3_endpoint: str | None = None,
    s3_bucket: str | None = None,
    s3_access_key: str | None = None,
    s3_secret_key: str | None = None,
    s3_region: str | None = None,
):
    BotoSession().client(
        aws_access_key_id=s3_access_key,
        aws_secret_access_key=s3_secret_key,
        region_name=s3_region,
        endpoint_url=s3_endpoint,
        service_name="s3",
    ).upload_file(file, s3_bucket, Path(file).name)


if __name__ == "__main__":
    main()
