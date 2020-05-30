from typing import Any, Dict, Optional, List

from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk


class ElasticsearchClient:
    def __init__(self) -> None:
        self._client: Optional[Elasticsearch] = None
        self.indices: List[str] = []

    @property
    def client(self) -> Elasticsearch:
        if self._client is None:
            self._client = Elasticsearch()
        return self._client

    def create_index(self, index: str, body: Optional[Dict[str, Any]] = None) -> None:
        """
        Creates an index in Elasticsearch if one isn't already there.
        """
        if body is None:
            body = {}
        self.client.indices.create(index=index, body=body, ignore=400)
        self.indices.append(index)

    def parallel_bulk(self, *args, **kwargs):
        """
        See https://elasticsearch-py.readthedocs.io/en/master/helpers.html#elasticsearch.helpers.parallel_bulk
        for details, this is just a wrapper method to unify the helper method and client.
        """
        parallel_bulk(self.client, *args, **kwargs)
