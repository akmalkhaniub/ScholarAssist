import xml.etree.ElementTree as ET
import pytest
from src.connectors.arxiv import ArxivConnector
from src.connectors.base import SourceName

SAMPLE_ATOM_FEED = b"""<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:arxiv="http://arxiv.org/schemas/atom">
  <entry>
    <id>http://arxiv.org/abs/2301.00001v1</id>
    <published>2023-01-01T12:00:00Z</published>
    <updated>2023-01-02T12:00:00Z</updated>
    <title>Scaling Laws for Multi-Agent Architectures</title>
    <summary>We examine scaling laws across multi-agent consensus protocols.</summary>
    <author>
      <name>Alice Smith</name>
    </author>
    <author>
      <name>Bob Jones</name>
    </author>
    <arxiv:doi>10.1234/arxiv.2301.00001</arxiv:doi>
    <category term="cs.AI" scheme="http://arxiv.org/schemas/atom"/>
    <category term="cs.MA" scheme="http://arxiv.org/schemas/atom"/>
  </entry>
</feed>
"""


def test_arxiv_connector_initialization():
    connector = ArxivConnector()
    assert connector.source_name == SourceName.ARXIV
    assert "arxiv.org" in connector.base_url


def test_arxiv_parse_feed():
    connector = ArxivConnector()
    records = connector.parse_feed(SAMPLE_ATOM_FEED)

    assert len(records) == 1
    rec = records[0]

    assert rec["source"] == "arxiv"
    assert rec["source_id"] == "2301.00001v1"
    assert rec["title"] == "Scaling Laws for Multi-Agent Architectures"
    assert rec["publication_year"] == "2023"
    assert len(rec["authors"]) == 2
    assert rec["authors"][0]["name"] == "Alice Smith"
    assert rec["authors"][1]["name"] == "Bob Jones"
    assert rec["doi"] == "10.1234/arxiv.2301.00001"
    assert "cs.AI" in rec["categories"]
    assert "cs.MA" in rec["categories"]
    assert "consensus" in rec["abstract"]
    assert rec["venue"]["name"] == "arXiv:2301.00001v1"
