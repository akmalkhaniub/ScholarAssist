import pytest
from src.bibliography.formatter import format_citations


def test_format_citations_single_author():
    records = [
        {
            "title": "Attention Is All You Need",
            "authors": [{"name": "Ashish Vaswani"}],
            "publication_year": "2017",
            "venue": {"name": "NeurIPS"},
            "doi": "10.5555/3295222.3295349",
        }
    ]

    apa = format_citations(records, style="apa")
    assert len(apa) == 1
    assert apa[0] == "Ashish Vaswani (2017). Attention Is All You Need. NeurIPS. https://doi.org/10.5555/3295222.3295349"

    mla = format_citations(records, style="mla")
    assert mla[0] == "Ashish Vaswani. \"Attention Is All You Need.\" NeurIPS, 2017."

    ieee = format_citations(records, style="ieee")
    assert ieee[0] == "Ashish Vaswani, \"Attention Is All You Need,\" NeurIPS, 2017."


def test_format_citations_two_authors():
    records = [
        {
            "title": "Deep Residual Learning for Image Recognition",
            "authors": [{"name": "Kaiming He"}, {"name": "Xiangyu Zhang"}],
            "publication_year": "2016",
            "venue": {"name": "CVPR"},
        }
    ]

    apa = format_citations(records, style="apa")
    assert apa[0] == "Kaiming He & Xiangyu Zhang (2016). Deep Residual Learning for Image Recognition. CVPR."


def test_format_citations_multiple_authors_et_al():
    records = [
        {
            "title": "Language Models are Few-Shot Learners",
            "authors": [
                {"name": "Tom B. Brown"},
                {"name": "Benjamin Mann"},
                {"name": "Nick Ryder"},
            ],
            "publication_year": "2020",
            "venue": {"name": "NeurIPS"},
        }
    ]

    apa = format_citations(records, style="apa")
    assert apa[0] == "Tom B. Brown et al. (2020). Language Models are Few-Shot Learners. NeurIPS."


def test_format_citations_empty_authors_fallback():
    records = [
        {
            "title": "Anonymous Whitepaper",
            "authors": [],
            "publication_year": "2024",
            "venue": {"name": "Crypto Archive"},
        }
    ]

    apa = format_citations(records, style="apa")
    assert apa[0] == "Unknown Author (2024). Anonymous Whitepaper. Crypto Archive."
