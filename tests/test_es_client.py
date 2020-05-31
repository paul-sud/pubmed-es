import pytest

from pubmed_es.es_client import ElasticsearchClient


def gendata():
    mywords = ["foo", "bar"]
    for i, word in enumerate(mywords):
        yield {"_index": "foo", "_id": i, "word": word}


@pytest.mark.integration
def test_es_client_parallel_bulk(elasticsearch_server):
    ec = ElasticsearchClient()
    ec.client.indices.create("foo")
    ec.parallel_bulk(gendata())
    assert ec.client.get(index="foo", id="0")["_source"] == {"word": "foo"}
    assert ec.client.get(index="foo", id="1")["_source"] == {"word": "bar"}
