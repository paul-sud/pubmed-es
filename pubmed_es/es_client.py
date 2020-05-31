from collections import deque
from typing import List, Optional

from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk as _parallel_bulk


class ElasticsearchClient:
    def __init__(self) -> None:
        self._client: Optional[Elasticsearch] = None
        self.indices: List[str] = []

    @property
    def client(self) -> Elasticsearch:
        if self._client is None:
            self._client = Elasticsearch()
        return self._client

    def parallel_bulk(self, *args, **kwargs):
        """
        See https://elasticsearch-py.readthedocs.io/en/master/helpers.html#elasticsearch.helpers.parallel_bulk
        for details, this is just a wrapper method to unify the helper method and client.

        `parallel_bulk` returns a generator, need to `deque` it to consume.
        """
        deque(_parallel_bulk(self.client, *args, **kwargs), maxlen=0)
