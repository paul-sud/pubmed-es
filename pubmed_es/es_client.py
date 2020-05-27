from typing import Any, Dict, List, Optional

from elasticsearch import Elasticsearch


class ElasticsearchClient:
    def __init__(self) -> None:
        self._client: Optional[Elasticsearch] = None
        self.indices: List[str] = []

    @property
    def client(self) -> Elasticsearch:
        if self._client is None:
            self._client = Elasticsearch()
        return self._client

    def create_index(self, name: str, mappings: Dict[str, Any]) -> None:
        self.client.indices.create(name, body={"mappings": mappings})
        self.indices.append(name)
