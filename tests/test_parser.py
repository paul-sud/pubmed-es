from collections import OrderedDict
from pathlib import Path
from unittest.mock import mock_open

import pytest

from pubmed_es.parser import (
    get_documents_from_pubmed_xml,
    get_documents_from_pubmed_xmls,
    pubmed_date_to_str,
    pythonify_key,
    selectively_flatten_dict,
)


def test_get_documents_from_pubmed_xml(mocker, articles):
    mocker.patch("builtins.open", mock_open(read_data=articles))
    pubmed_articles = get_documents_from_pubmed_xml(Path("foo"))
    assert len(pubmed_articles) == 1
    assert (
        pubmed_articles[0]["abstract_text"]
        == "(--)-alpha-Bisabolol has a primary antipeptic action"
    )
    assert pubmed_articles[0]["mesh_headings"] == [
        {
            "major_topic": False,
            "text": "Dose-Response Relationship, Drug",
            "ui": "D004305",
        }
    ]
    assert pubmed_articles[0]["chemicals"] == [{"text": "Hemoglobins", "ui": "D006454"}]
    assert pubmed_articles[0]["date_completed"] == "1975-09-01"
    assert "history" not in pubmed_articles[0]
    assert "pubmed_article" not in pubmed_articles[0]


def test_get_documents_from_pubmed_xmls(mocker):
    mocker.patch("pathlib.Path.glob", return_value=["bar", "baz"])
    mocker.patch(
        "pubmed_es.parser.get_documents_from_pubmed_xml",
        side_effect=[[{"foo": "bar"}], [{"baz": "qux"}]],
    )
    pubmed_articles = get_documents_from_pubmed_xmls(Path("foo"))
    assert next(pubmed_articles) == {"foo": "bar"}
    assert next(pubmed_articles) == {"baz": "qux"}


def test_selectively_flatten_dict():
    data = OrderedDict({
        "foo": OrderedDict({"Bar": "baz"}),
        "qux": [1, 2, 3],
        "#spam": [OrderedDict({"eggs": OrderedDict({"quux": "corge"})})],
        "grault": "1",
        "date": OrderedDict({"Year": "1999", "Month": "03", "Day": "05"}),
    })
    result = selectively_flatten_dict(
        data, date_fields=("date",), ignore_keys=("grault",)
    )
    assert result == {
        "bar": "baz",
        "quxs": [1, 2, 3],
        "spams": [{"quux": "corge"}],
        "date": "1999-03-05",
    }


@pytest.mark.parametrize("key,expected", [("@foo", "foo"), ("BazQux", "baz_qux")])
def test_pythonify_key(key: str, expected: str):
    result = pythonify_key(key)
    assert result == expected


def test_pubmed_date_to_str():
    data = {"Year": "1972", "Month": "03", "Day": "20"}
    result = pubmed_date_to_str(data)
    assert result == "1972-03-20"
