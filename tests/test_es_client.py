import subprocess
import sys
from pathlib import Path

import pytest


def server_process(datadir: Path, host="127.0.0.1", port=9201, echo=False):
    args = [
        "elasticsearch",
        f"-Enetwork.host={host}",
        f"-Ehttp.port={port}",
        f"-Epath.data={datadir / 'data'}",
        f"-Epath.logs={datadir / 'logs'}",
    ]
    process = subprocess.Popen(
        args, close_fds=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )

    SUCCESS_LINE = b"started\n"

    lines = []

    for line in iter(process.stdout.readline, b""):  # type: ignore
        if echo:
            sys.stdout.write(line.decode("utf-8"))
        lines.append(line)
        if line.endswith(SUCCESS_LINE):
            break
    else:
        code = process.wait()
        msg = ("Process return code: %d\n" % code) + b"".join(lines).decode("utf-8")
        raise Exception(msg)

    if not echo:
        process.stdout.close()  # type: ignore
    return process


@pytest.yield_fixture(scope="session")
def elasticsearch_server(tmp_path, elasticsearch_host_port):
    host, port = elasticsearch_host_port
    process = server_process(tmp_path, echo=True)

    yield "http://127.0.0.1:9201"

    if "process" in locals() and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
