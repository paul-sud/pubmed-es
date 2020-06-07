import pytest

from pubmed_es.__main__ import main
from pubmed_es.es_client import ElasticsearchClient


@pytest.mark.integration
def test_main(mocker, elasticsearch_server, article):
    mocker.patch("sys.argv", return_value=["-d", "data"])
    main()
    ec = ElasticsearchClient()
    assert ec.client.indices.exists(index="pubmed")
    assert ec.client.get(index="pubmed", id=0)["_source"]["pmid"] == "foo"
