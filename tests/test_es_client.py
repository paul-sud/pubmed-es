import subprocess
from time import sleep

from elasticsearch import Elasticsearch
import pytest


@pytest.fixture(scope="session")
def server_process(tmp_path):
    args = [
        "elasticsearch",
        "-Enetwork.host=127.0.0.1",
        "-Ehttp.port=9201",
        f"-Epath.data={tmp_path / 'data'}",
        f"-Epath.logs={tmp_path / 'logs'}",
    ]
    process = subprocess.run(
        args, close_fds=True, stderr=subprocess.STDOUT
    )

    es = Elasticsearch()
    retries_left = 3
    while True:
        health = es.cluster.health()
        if health.get("status") != "red":
            break
        if retries_left == 0:
            terminate_process(process)
            raise RuntimeError("Elasticsearch failed to start")
        retries_left -= 1
        sleep(10)

    del es

    yield "http://127.0.0.1:9201"

    terminate_process(process)


def terminate_process(process):
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
