import subprocess
from time import sleep

import pytest
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError


@pytest.fixture(scope="session")
def elasticsearch_server(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("es")
    args = [
        "elasticsearch",
        "-Enetwork.host=127.0.0.1",
        "-Ehttp.port=9200",
        f"-Epath.data={tmp_path / 'data'}",
        f"-Epath.logs={tmp_path / 'logs'}",
    ]
    process = subprocess.Popen(args, close_fds=True, stderr=subprocess.STDOUT)

    es = Elasticsearch()
    retries_left = 5
    backoff_time = 1
    while True:
        try:
            health = es.cluster.health()
            if health.get("status") != "red":
                break
        except ConnectionError:
            pass
        if retries_left == 0:
            terminate_process(process)
            raise RuntimeError("Elasticsearch failed to start")
        sleep(backoff_time)
        retries_left -= 1
        backoff_time *= 2

    del es

    yield "http://127.0.0.1:9201"

    terminate_process(process)


def terminate_process(process):
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
