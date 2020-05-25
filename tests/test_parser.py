import gzip
from collections import OrderedDict
from pathlib import Path
from unittest.mock import mock_open

import pytest

from pubmed_es.parser import (
    get_documents_from_pubmed_xml,
    ordered_dict_to_dict,
    pythonify_key,
    selectively_flatten_dict,
)

article = """
<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation Status="MEDLINE" Owner="NLM">
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
    assert pubmed_articles[0]["mesh_heading_list"] == [
        {
            "major_topic_y_n": "N",
            "text": "Dose-Response Relationship, Drug",
            "u_i": "D004305",
        }
    ]
    assert pubmed_articles[0]["chemical_list"] == [
        {"text": "Hemoglobins", "u_i": "D006454"}
    ]
    assert pubmed_articles[0]["pubmed_pub_date"] == "1975-9-1"


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
    }
    result = selectively_flatten_dict(data)
    assert result == {"bar": "baz", "qux": [1, 2, 3], "spam": [{"quux": "corge"}]}


@pytest.mark.parametrize("key,expected", [("@foo", "foo"), ("BazQux", "baz_qux")])
def test_pythonify_key(key, expected):
    result = pythonify_key(key)
    assert result == expected
