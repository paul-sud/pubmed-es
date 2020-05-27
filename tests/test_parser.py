import gzip
from collections import OrderedDict
from pathlib import Path
from unittest.mock import mock_open

import pytest

from pubmed_es.parser import (
    get_documents_from_pubmed_xml,
    ordered_dict_to_dict,
    pubmed_date_to_str,
    pythonify_key,
    selectively_flatten_dict,
)

article = """
<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation Status="MEDLINE" Owner="NLM">
      <DateCompleted>
        <Year>1975</Year>
        <Month>09</Month>
        <Day>01</Day>
      </DateCompleted>
      <Article PubModel="Print">
        <Journal>
          <Title>Arzneimittel-Forschung</Title>
        </Journal>
        <ArticleTitle>[Biochemical studies].</ArticleTitle>
        <Abstract>
          <AbstractText>(--)-alpha-Bisabolol has a primary antipeptic action</AbstractText>
        </Abstract>
        <AuthorList CompleteYN="Y">
          <Author ValidYN="Y">
            <LastName>Isaac</LastName>
            <ForeName>O</ForeName>
            <Initials>O</Initials>
          </Author>
        </AuthorList>
        <PublicationTypeList>
          <PublicationType UI="D004740">English Abstract</PublicationType>
        </PublicationTypeList>
      </Article>
      <ChemicalList>
        <Chemical>
          <NameOfSubstance UI="D006454">Hemoglobins</NameOfSubstance>
        </Chemical>
      </ChemicalList>
      <MeshHeadingList>
        <MeshHeading>
          <DescriptorName UI="D004305" MajorTopicYN="N">Dose-Response Relationship, Drug</DescriptorName>
        </MeshHeading>
      </MeshHeadingList>
    </MedlineCitation>
    <PubmedData>
      <History>
        <PubMedPubDate PubStatus="pubmed">
          <Year>1975</Year>
          <Month>9</Month>
          <Day>1</Day>
        </PubMedPubDate>
      </History>
      <ArticleIdList>
        <ArticleId IdType="pubmed">21</ArticleId>
      </ArticleIdList>
    </PubmedData>
  </PubmedArticle>
</PubmedArticleSet>
"""


def test_get_documents_from_pubmed_xml(mocker):
    data = gzip.compress(bytes(article, "utf-8"))
    mocker.patch("builtins.open", mock_open(read_data=data))
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


def test_ordered_dict_to_dict():
    ordered = OrderedDict()
    ordered.update({"1": 2, "3": OrderedDict({"foo": "bar"})})
    result = ordered_dict_to_dict(ordered)
    assert result == {"1": 2, "3": {"foo": "bar"}}


def test_selectively_flatten_dict():
    data = {
        "foo": {"Bar": "baz"},
        "qux": [1, 2, 3],
        "#spam": [{"eggs": {"quux": "corge"}}],
        "grault": "1",
        "date": {"Year": "1999", "Month": "03", "Day": "05"},
    }
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
