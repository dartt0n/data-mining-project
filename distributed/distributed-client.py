# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "polars>=1.26.0",
#     "pyarrow>=19.0.1",
#     "pydantic>=2.11.2",
#     "pymongo>=4.11.3",
#     "rich>=14.0.0",
#     "click>=8.1.8",
#     "httpx>=0.25.0",
#     "boto3>=1.35.99",
#     "botocore>=1.35.0,<1.36",
# ]
# ///

from contextlib import contextmanager
import os
from typing import Any
import click
import httpx
import pymongo
import polars as pl
import rich
from boto3.session import Session as BotoSession
import rich.progress  # type: ignore

SCHEMA = pl.Schema(
    {
        "work_id": pl.String,
        "doi": pl.String,
        "title": pl.String,
        "date": pl.String,
        "authors": pl.List(
            pl.Struct(
                {
                    "id": pl.String,
                    "name": pl.String,
                    "raw_name": pl.String,
                    "country_codes": pl.List(pl.String),
                    "institutions": pl.List(
                        pl.Struct(
                            {
                                "id": pl.String,
                                "name": pl.String,
                                "country_code": pl.String,
                            }
                        )
                    ),
                }
            )
        ),
        "concepts": pl.List(
            pl.Struct(
                {
                    "id": pl.String,
                    "name": pl.String,
                    "score": pl.Float32,
                }
            )
        ),
        "topics": pl.List(
            pl.Struct(
                {
                    "id": pl.String,
                    "name": pl.String,
                    "domain": pl.String,
                    "field": pl.String,
                }
            )
        ),
        "referenced_works": pl.List(pl.String),
        "related_works": pl.List(pl.String),
        "keywords": pl.List(pl.String),
        "citations": pl.List(
            pl.Struct(
                {
                    "year": pl.Int32,
                    "count": pl.Int32,
                }
            )
        ),
    }
)


@click.command()
@click.option("--server", default="http://127.0.0.1:10509")
@click.option("--s3-endpoint")
@click.option("--s3-bucket")
@click.option("--s3-access-key")
@click.option("--s3-secret-key")
@click.option("--s3-region")
@click.option("--mongo-uri", default="mongodb://localhost:27017")
def main(
    server: str,
    s3_endpoint: str | None,
    s3_bucket: str | None,
    s3_access_key: str | None,
    s3_secret_key: str | None,
    s3_region: str | None,
    mongo_uri: str,
) -> None:
    console = rich.console.Console()

    @contextmanager  # type: ignore
    def status(task: str) -> None:  # type: ignore
        with console.status(task):
            yield  # type: ignore

        console.print(f"[green]✔ {task}")

    with status("connect to server"):
        http_client = httpx.Client(base_url=server)
        http_client.get("/status").raise_for_status()

    with status("connect to s3"):
        s3_client = BotoSession().client(  # type: ignore
            aws_access_key_id=s3_access_key,
            aws_secret_access_key=s3_secret_key,
            region_name=s3_region,
            endpoint_url=s3_endpoint,
            service_name="s3",
        )

    with status("connect to mongodb"):
        mongo_client = pymongo.MongoClient(mongo_uri)  # type: ignore
        alexdata = mongo_client.get_database("data-mining").get_collection("alexdata")  # type: ignore

    while True:
        with console.status("job request"):
            response = http_client.get("/job")
            if response.status_code == 404:
                console.print("[yellow]• no jobs available, exiting")
                break

            job = response.json()
            key = job["key"]
            head = job["head"]
            tail = job["tail"]
            file = f"alexdata_{head:09d}_{tail:09d}.parquet"

            console.print(
                f"[blue]• got job <{key}>: process 'alexdata[{head}:{tail}]' as '{file}'"
            )

        try:
            collected_data: list[dict[str, Any]] = []

            with status("cursor creation"):
                cursor = alexdata.find(  # type: ignore
                    {"rowId": {"$gte": head, "$lte": tail}},
                ).limit(tail - head)

            with rich.progress.Progress(console=console) as p:
                for row in p.track(  # type: ignore
                    cursor,  # type: ignore
                    description="collecting data in memory",
                    total=tail - head,
                ):
                    row.pop("_id", None)  # type: ignore
                    row.pop("rowId", None)  # type: ignore
                    collected_data.append(row)  # type: ignore

            with status("polars dataframe build"):
                df = pl.from_dicts(collected_data, strict=False, schema=SCHEMA)
                collected_data.clear()
                del collected_data

            with status("optimize dataframe size"):
                df = (
                    df.lazy()
                    .with_columns(
                        # work id
                        (pl.col("work_id").str.strip_prefix("https://openalex.org/")),
                        # doi
                        (
                            pl.when(pl.col("doi").is_null())
                            .then(pl.lit(""))
                            .otherwise(pl.col("doi"))
                            .str.strip_prefix("https://doi.org/")
                        ),
                        # authors
                        (
                            pl.col("authors").list.eval(
                                pl.element().struct.with_fields(
                                    pl.field("id").str.strip_prefix(
                                        "https://openalex.org/"
                                    ),
                                    pl.field("institutions").list.eval(
                                        pl.element().struct.with_fields(
                                            pl.field("id").str.strip_prefix(
                                                "https://openalex.org/"
                                            )
                                        )
                                    ),
                                ),
                            )
                        ),
                        # topics
                        (
                            pl.col("topics").list.eval(
                                pl.element().struct.with_fields(
                                    pl.field("id").str.strip_prefix(
                                        "https://openalex.org/"
                                    )
                                )
                            )
                        ),
                        # referenced works
                        (
                            pl.col("referenced_works").list.eval(
                                pl.element().str.strip_prefix("https://openalex.org/")
                            )
                        ),
                        # related works
                        (
                            pl.col("related_works").list.eval(
                                pl.element().str.strip_prefix("https://openalex.org/")
                            )
                        ),
                        # concepts
                        (
                            pl.col("concepts").list.eval(
                                pl.element().struct.with_fields(
                                    pl.field("id").str.strip_prefix(
                                        "https://openalex.org/"
                                    )
                                )
                            )
                        ),
                    )
                    .collect()
                )

            with status("dataframe compression"):
                df.write_parquet(file, compression="zstd", compression_level=17)

            with status("file upload to s3"):
                s3_client.upload_file(file, s3_bucket, file)  # type: ignore

            with status("local file removal"):
                os.remove(file)

        except (EOFError, KeyboardInterrupt):
            console.print("[red] ! terminated by user")

            with status("job failure report"):
                http_client.post("/fail", json={"key": key})

            exit(1)

        except Exception as e:
            console.print(f"[red] ! failed to collect data: {e}")

            with status("job failure report"):
                http_client.post("/fail", json={"key": key})
                continue
        else:
            with status("job success report"):
                http_client.post("/done", json={"key": key})
                continue


if __name__ == "__main__":
    main()
