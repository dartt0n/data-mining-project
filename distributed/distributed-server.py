# /// script
# requires-python = ">=3.12"
# dependencies = [
#    "click>=8.1.8",
#    "sanic>=25.3.0",
# ]
# ///


import asyncio
from dataclasses import dataclass, asdict
from types import SimpleNamespace
from uuid import uuid4
from sanic import Request, Sanic
from sanic.response import json
from sanic.config import Config as SanicConfig


app = Sanic("distributed-server")


class Config:
    max_slice_size = 500_000


@dataclass(frozen=True)
class JobItem:
    key: str
    head: int
    tail: int


@dataclass(frozen=True)
class ManagerStatus:
    backlog: int
    active: int


class JobManager:
    def __init__(self):
        self._backlog: set[JobItem] = set()
        self._active_jobs: dict[str, JobItem] = {}
        self._lock = asyncio.Lock()

    async def init(self):
        total_count = 268_761_173

        async with self._lock:
            self._backlog = set()
            for i in range(0, total_count, Config.max_slice_size):
                self._backlog.add(
                    JobItem(
                        key=uuid4().hex,
                        head=i,
                        tail=min(i + Config.max_slice_size, total_count),
                    )
                )

    async def take(self) -> JobItem | None:
        async with self._lock:
            if len(self._backlog) == 0:
                return None

            job = self._backlog.pop()
            self._active_jobs[job.key] = job
            return job

    async def commit(self, key: str):
        async with self._lock:
            self._active_jobs.pop(key)

    async def release(self, key: str):
        async with self._lock:
            if key not in self._active_jobs:
                return

            slice = self._active_jobs.pop(key)
            self._backlog.add(slice)

    async def status(self) -> ManagerStatus:
        async with self._lock:
            return ManagerStatus(
                backlog=len(self._backlog),
                active=len(self._active_jobs),
            )


manager = JobManager()


@app.before_server_start
async def init(app: Sanic[SanicConfig, SimpleNamespace], *_):
    await manager.init()


@app.get("/status")
async def get_status(request: Request):
    status = await manager.status()
    return json(asdict(status), status=200)


@app.get("/job")
async def get_slice(request: Request):
    job = await manager.take()
    if job is None:
        return json({"error": "no slice available"}, status=404)

    return json(asdict(job), status=200)


@app.post("/done")
async def commit_slice(request: Request):
    if not request.json:
        return json({"error": "request must be json"}, status=400)

    key = request.json.get("key")
    if key is None:
        return json({"error": "no key provided"}, status=400)

    await manager.commit(key)
    return json({"success": True})


@app.post("/fail")
async def release_slice(request: Request):
    if not request.json:
        return json({"error": "request must be json"}, status=400)

    key = request.json.get("key")
    if key is None:
        return json({"error": "no key provided"}, status=400)

    await manager.release(key)
    return json({"success": True})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, workers=1)  # type: ignore
