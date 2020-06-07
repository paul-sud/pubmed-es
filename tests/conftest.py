import gzip
import subprocess
from time import sleep

import pytest
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError


@pytest.fixture(scope="session")
def elasticsearch_server(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("es")
    args = [
        "elasticsearch",
        "-Enetwork.host=127.0.0.1",
        "-Ehttp.port=9200",
        f"-Epath.data={tmp_path / 'data'}",
        f"-Epath.logs={tmp_path / 'logs'}",
    ]
    process = subprocess.Popen(args, close_fds=True, stderr=subprocess.STDOUT)

    es = Elasticsearch()
    retries_left = 5
    backoff_time = 1
    while True:
        try:
            health = es.cluster.health()
            if health.get("status") != "red":
                break
        except ConnectionError:
            pass
        if retries_left == 0:
            terminate_process(process)
            raise RuntimeError("Elasticsearch failed to start")
        sleep(backoff_time)
        retries_left -= 1
        backoff_time *= 2

    del es

    yield "http://127.0.0.1:9201"

    terminate_process(process)


def terminate_process(process):
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()


@pytest.fixture
def articles():
    data = """
<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation Status="MEDLINE" Owner="NLM">
      <PMID Version="1">1</PMID>
      <DateCompleted>
        <Year>1975</Year>
        <Month> 09</Month>
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
    return gzip.compress(bytes(data, "utf-8"))
