import argparse
from pathlib import Path

from elasticsearch.exceptions import ConnectionError

from pubmed_es.es_client import ElasticsearchClient
from pubmed_es.parser import get_documents_from_pubmed_xmls


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        "--data-dir",
        required=True,
        help="Path to directory containing Pubmed XML data",
    )
    return parser


def main() -> None:
    parser = get_parser()
    args = parser.parse_args()
    ec = ElasticsearchClient()
    try:
        ec.client.cluster.health()
    except ConnectionError as e:
        raise RuntimeError("Elasticsearch is not running") from e
    ec.parallel_bulk(get_documents_from_pubmed_xmls(Path(args.data_dir)))


if __name__ == "__main__":
    main()
