"""Unit tests for ScholarAssist normalization, deduplication, and Airflow DAG definitions."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

from src.deduplication.confidence import score_match_confidence
from src.deduplication.golden_record import resolve_golden_records
from src.normalization.cleaners import (
    clean_doi,
    clean_title,
    extract_first_author_lastname,
    normalize_author_name,
    strip_html,
)

REPO_ROOT = Path(__file__).resolve().parent.parent


def _install_airflow_stubs(monkeypatch) -> dict[str, list[Any]]:
    """Register minimal Airflow stubs in sys.modules so DAG files load without a live cluster."""
    recorded: dict[str, list[Any]] = {"dags": [], "operators": []}

    class FakeOperator:
        def __init__(self, task_id: str, **kwargs: Any) -> None:
            self.task_id = task_id
            self.kwargs = kwargs
            self.downstream_ids: list[str] = []
            recorded["operators"].append(self)

        def __rshift__(self, other: Any) -> Any:
            targets = other if isinstance(other, list) else [other]
            for target in targets:
                self.downstream_ids.append(target.task_id)
            return other

        def __rrshift__(self, other: Any) -> Any:
            sources = other if isinstance(other, list) else [other]
            for src in sources:
                src.downstream_ids.append(self.task_id)
            return self

    class FakeDAG:
        def __init__(self, dag_id: str, **kwargs: Any) -> None:
            self.dag_id = dag_id
            self.kwargs = kwargs
            recorded["dags"].append(self)

        def __enter__(self) -> FakeDAG:
            return self

        def __exit__(self, *args: Any) -> None:
            return None

    airflow_mod = ModuleType("airflow")
    airflow_mod.DAG = FakeDAG  # type: ignore[attr-defined]

    operators_mod = ModuleType("airflow.operators")
    empty_mod = ModuleType("airflow.operators.empty")
    empty_mod.EmptyOperator = FakeOperator  # type: ignore[attr-defined]

    providers_mod = ModuleType("airflow.providers")
    apache_mod = ModuleType("airflow.providers.apache")
    spark_mod = ModuleType("airflow.providers.apache.spark")
    spark_ops_mod = ModuleType("airflow.providers.apache.spark.operators")
    spark_submit_mod = ModuleType("airflow.providers.apache.spark.operators.spark_submit")
    spark_submit_mod.SparkSubmitOperator = FakeOperator  # type: ignore[attr-defined]

    for name, mod in {
        "airflow": airflow_mod,
        "airflow.operators": operators_mod,
        "airflow.operators.empty": empty_mod,
        "airflow.providers": providers_mod,
        "airflow.providers.apache": apache_mod,
        "airflow.providers.apache.spark": spark_mod,
        "airflow.providers.apache.spark.operators": spark_ops_mod,
        "airflow.providers.apache.spark.operators.spark_submit": spark_submit_mod,
    }.items():
        monkeypatch.setitem(sys.modules, name, mod)

    return recorded


def _load_dag_module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_doi_and_title_normalization():
    assert clean_doi("HTTPS://DOI.ORG/10.1234/ABC.def ") == "10.1234/abc.def"
    assert clean_doi("http://dx.doi.org/10.5555/TEST") == "10.5555/test"
    assert clean_doi(" doi:10.1109/CVPR.2026 ") == "10.1109/cvpr.2026"
    assert clean_doi("not-a-doi") is None
    assert clean_doi(None) is None

    assert strip_html("<p>Deep <i>Learning</i></p>") == "Deep Learning"
    assert strip_html(None) is None
    assert clean_title("<b>Attention</b> Is All You Need: A Survey!") == "attention is all you need a survey"


def test_author_name_cleansing_and_blocking_key():
    assert normalize_author_name("Dr. Jane   Doe") == "Jane Doe"
    assert normalize_author_name("Vaswani, Ashish") == "Ashish Vaswani"
    assert normalize_author_name("Prof. Maria-Elena Garcia") == "Maria-Elena Garcia"
    assert normalize_author_name(None) == ""

    authors = [{"name": "Vaswani, Ashish"}, {"name": "Noam Shazeer"}]
    assert extract_first_author_lastname(authors) == "vaswani"
    assert extract_first_author_lastname([{"name": "Dr. Jean-Luc O'Connor"}]) == "oconnor"
    assert extract_first_author_lastname([]) is None


def test_deduplication_confidence_and_golden_record_merge():
    assert score_match_confidence("exact_doi") == 1.0
    assert score_match_confidence("normalized_doi") == 0.95
    assert score_match_confidence("lsh_match") == 0.85
    assert score_match_confidence("unknown") == 0.5

    records = [
        {
            "id": "oa-1",
            "provider": "OpenAlex",
            "title": "Attention Is All You Need",
            "doi": None,
            "publication_year": 2017,
            "first_author_lastname": "vaswani",
        },
        {
            "id": "cr-1",
            "provider": "Crossref",
            "title": "Attention is all you need",
            "doi": "10.48550/arXiv.1706.03762",
            "publication_year": None,
            "first_author_lastname": "vaswani",
        },
        {
            "id": "s2-9",
            "provider": "Semantic Scholar",
            "title": "Unclustered Paper",
            "doi": "10.1000/xyz",
            "publication_year": 2024,
            "first_author_lastname": "smith",
        },
    ]
    cluster_mapping = {"oa-1": "cluster-1", "cr-1": "cluster-1"}
    golden = resolve_golden_records(
        records,
        cluster_mapping,
        provider_priority=["Crossref", "OpenAlex", "Semantic Scholar"],
    )

    by_id = {g["golden_record_id"]: g for g in golden}
    assert set(by_id.keys()) == {"cluster-1", "s2-9"}

    merged = by_id["cluster-1"]
    assert merged["title"] == "Attention is all you need"  # Crossref wins priority
    assert merged["doi"] == "10.48550/arXiv.1706.03762"
    assert merged["publication_year"] == 2017  # Falls back to OpenAlex when Crossref is None
    assert merged["source_provenance"]["title"] == "Crossref"
    assert merged["source_provenance"]["publication_year"] == "OpenAlex"
    assert set(merged["merged_ids"]) == {"oa-1", "cr-1"}


def test_normalization_and_deduplication_dags_load_portably(monkeypatch):
    recorded = _install_airflow_stubs(monkeypatch)
    monkeypatch.setenv("SCHOLARASSIST_HOME", "/srv/scholarassist")

    _load_dag_module("normalization_dag_test", REPO_ROOT / "dags" / "normalization_dag.py")
    _load_dag_module("deduplication_dag_test", REPO_ROOT / "dags" / "deduplication_dag.py")

    dag_ids = [d.dag_id for d in recorded["dags"]]
    assert "normalization_pipeline" in dag_ids
    assert "deduplication_dag" in dag_ids

    ops_by_id = {op.task_id: op for op in recorded["operators"]}
    for expected_task in (
        "normalize_openalex",
        "normalize_crossref",
        "normalize_semantic_scholar",
        "trigger_deduplication",
        "run_deduplication_job",
    ):
        assert expected_task in ops_by_id

    dedup_op = ops_by_id["run_deduplication_job"]
    assert dedup_op.kwargs["application"] == "/srv/scholarassist/src/deduplication/job.py"
    assert not any("g:/" in str(arg).lower() for arg in dedup_op.kwargs["application_args"])
