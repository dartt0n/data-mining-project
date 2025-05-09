# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "polars>=1.26.0",
#     "pyarrow>=19.0.1",
#     "rich>=14.0.0",
#     "boto3>=1.35.99",
#     "pyyaml>=6.0.2",
#     "botocore>=1.35.0,<1.36",
#     "pydantic>=2.11.2",
#     "fsspec", "s3fs", "adlfs"
# ]
# ///


from __future__ import annotations
import gzip
import json
import os
import random
import string
import multiprocessing
from pathlib import Path
import re
import yaml
from typing import Any, Literal, Mapping
from pydantic import BaseModel
import polars as pl
from concurrent.futures import ProcessPoolExecutor
import sys

BATCH_SIZE = 2_000_000_000  # 2 GB
NUM_CORES = multiprocessing.cpu_count()  # or const number


def deepgso(ob: Any):
    size = sys.getsizeof(ob)  # type: ignore
    if isinstance(ob, (list, tuple, set)):
        for element in ob:  # type: ignore
            size += deepgso(element)
    if isinstance(ob, dict):
        for k, v in ob.items():  # type: ignore
            size += deepgso(k)
            size += deepgso(v)
    return size


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


def jsonpath_to_python_key(path: str) -> list[int | str]:
    keys: list[int | str] = []
    for element in re.sub(r"\[(\d+)\]", r".\[\1\]", path).split("."):
        if element.startswith("["):
            keys.append(int(element[1:-1]))
        else:
            keys.append(element)

    return keys


def get(data: dict[str, Any] | list[Any] | Any, path: str) -> Any:
    if path == "":
        return data

    for key in jsonpath_to_python_key(path):
        if key in data:
            data = data[key]  # type: ignore
        else:
            return None

    return data  # type: ignore


class Extractor(BaseModel):
    fields: Mapping[str, Field]

    def example(self) -> dict[str, Any]:
        return {k: v.example() for k, v in self.fields.items()}

    def extract(self, data: dict[str, Any]) -> dict[str, Any]:
        return {k: v.extract(data) for k, v in self.fields.items()}  # type: ignore


class PlainField(BaseModel):
    desc: str
    type: Literal["string", "int", "float"]
    path: str

    def example(self) -> Any:
        if self.type == "string":
            return "string"
        elif self.type == "int":
            return 1
        elif self.type == "float":
            return 0.5

    def extract(self, data: dict[str, Any]) -> Any:
        if self.type == "string":
            return get(data, self.path)
        elif self.type == "int":
            return int(get(data, self.path))
        elif self.type == "float":
            return float(get(data, self.path))
        else:
            raise ValueError(f"Unknown type: {self.type}")


class ListField(BaseModel):
    desc: str
    type: Literal["list"]
    path: str
    map: Map

    def example(self) -> list[Any]:
        return [self.map.example() for _ in range(3)]  # type: ignore

    def extract(self, data: list[Any]) -> list[Any]:
        return [self.map.extract(d) for d in get(data, self.path)]


type Field = PlainField | ListField

type Map = StructMapping | FieldMapping


class StructMapping(BaseModel):
    struct: Mapping[str, Field]

    def example(self) -> dict[str, Any]:
        return {k: v.example() for k, v in self.struct.items()}

    def extract(self, data: dict[str, Any]) -> dict[str, Any]:
        return {k: v.extract(data) for k, v in self.struct.items()}  # type: ignore


class FieldMapping(BaseModel):
    field: Field

    def example(self) -> Any:
        return self.field.example()

    def extract(self, data: dict[str, Any]) -> Any:
        return self.field.extract(data)  # type: ignore


def generate_random_filename() -> str:
    letters = string.ascii_lowercase + string.digits
    random_part = "".join(random.choice(letters) for _ in range(16))
    return f"alexdata_{random_part}.parquet"


def process_file(file_path: Path, extractor: Extractor):
    batch: list[dict[str, Any]] = []
    try:
        with gzip.open(file_path, "rt") as f:
            for line in f:
                try:
                    batch.append(extractor.extract(json.loads(line.strip())))
                    if deepgso(batch) >= BATCH_SIZE:
                        process_batch(batch)
                        batch.clear()
                except Exception:
                    print(f"Error processing line at {file_path}")
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")

    if batch:
        process_batch(batch)


def process_batch(batch: list[dict[str, Any]]) -> None:
    pl.from_dicts(batch, schema=SCHEMA, strict=False).lazy().with_columns(
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
                    pl.field("id").str.strip_prefix("https://openalex.org/"),
                    pl.field("institutions").list.eval(
                        pl.element().struct.with_fields(
                            pl.field("id").str.strip_prefix("https://openalex.org/")
                        )
                    ),
                ),
            )
        ),
        # topics
        (
            pl.col("topics").list.eval(
                pl.element().struct.with_fields(
                    pl.field("id").str.strip_prefix("https://openalex.org/")
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
                    pl.field("id").str.strip_prefix("https://openalex.org/")
                )
            )
        ),
    ).sink_parquet(
        f"s3://data-mining/{generate_random_filename()}",
        compression="zstd",
        compression_level=17,
        storage_options=dict(
            aws_access_key_id=os.environ["ACCESS_TOKEN"],
            aws_secret_access_key=os.environ["SECRET_TOKEN"],
            region_name="ru-central1",
            endpoint_url="https://storage.yandexcloud.net",
            service_name="s3",
        ),
    )


def process_file_wrapper(args):
    file_path, extractor = args
    return process_file(file_path, extractor)


if __name__ == "__main__":
    with open("fields.yaml") as f:
        extractor = Extractor.model_validate(yaml.safe_load(f))

    files = sorted(Path().glob("data/works/**/*.gz"))
    print(f"Found {len(files)} files to process")

    with ProcessPoolExecutor(max_workers=NUM_CORES) as executor:
        args = [(file_path, extractor) for file_path in files]
        executor.map(process_file_wrapper, args)
