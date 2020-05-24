import gzip
from pathlib import Path
from unittest.mock import mock_open

from pubmed_es.parser import get_documents_from_pubmed_xml

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
        pubmed_articles[0]["PubmedArticle"]["MedlineCitation"]["Article"]["Abstract"][
            "AbstractText"
        ]
        == "(--)-alpha-Bisabolol has a primary antipeptic action"
    )
