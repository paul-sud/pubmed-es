import pytest

from pubmed_es.es_client import ElasticsearchClient


@pytest.mark.integration
def test_es_client_parallel_bulk(elasticsearch_server):
    ec = ElasticsearchClient()
    ec.client.indices.create("foo")

    def gendata():
        mywords = ["foo", "bar"]
        for i, word in enumerate(mywords):
            yield {
                "_index": "foo",
                "_id": str(i),
                "_type": "doc",
                "doc": {"word": word},
            }

    ec.parallel_bulk(gendata)
    assert ec.client.get(index="foo", id="0", doc_type="doc") == {"word": "foo"}
    assert ec.client.get(index="foo", id="1", doc_type="doc") == {"word": "bar"}
