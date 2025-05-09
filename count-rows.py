import os
import polars as pl


print(
    pl.scan_parquet(
        "s3://data-mining2/*",
        storage_options=dict(
            aws_access_key_id=os.environ["ACCESS_TOKEN"],
            aws_secret_access_key=os.environ["SECRET_TOKEN"],
            region_name="ru-central1",
            endpoint_url="https://storage.yandexcloud.net",
            service_name="s3",
        ),
    )
    .select(pl.len())
    .collect()
    .item()
)
