from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

import pytest

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.grafana.grafana_config import PlatformConnectionConfig
from datahub.ingestion.source.grafana.lineage import LineageExtractor
from datahub.ingestion.source.grafana.models import Panel
from datahub.ingestion.source.grafana.report import GrafanaSourceReport
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    UpstreamLineageClass,
)


@pytest.fixture
def mock_graph():
    return MagicMock()


@pytest.fixture
def mock_report():
    return GrafanaSourceReport()


@pytest.fixture
def lineage_extractor(mock_graph, mock_report):
    return LineageExtractor(
        platform="grafana",
        platform_instance="test-instance",
        env="PROD",
        connection_to_platform_map={
            "postgres_uid": PlatformConnectionConfig(
                platform="postgres",
                database="test_db",
                database_schema="public",
            ),
            "mysql_uid": PlatformConnectionConfig(
                platform="mysql",
                database="test_db",
            ),
        },
        report=mock_report,
        graph=mock_graph,
    )


def _make_panel(
    datasource: Dict[str, Any], targets: Optional[List[Dict[str, Any]]] = None
) -> Panel:
    return Panel(
        id="1",
        title="Test Panel",
        type="graph",
        datasource=datasource,
        targets=targets or [{"rawSql": "SELECT value FROM test_table"}],
    )


def test_extract_panel_lineage_no_datasource(lineage_extractor):
    panel = Panel(id="1", title="Test Panel", type="graph", datasource=None, targets=[])

    lineage = lineage_extractor.extract_panel_lineage(panel, "test-dashboard")
    assert lineage is None


def test_extract_panel_lineage_unknown_datasource(lineage_extractor):
    panel = _make_panel(
        datasource={"type": "unknown", "uid": "unknown_uid"},
        targets=[{"expr": "rate(http_requests_total[5m])"}],
    )

    lineage = lineage_extractor.extract_panel_lineage(panel, "test-dashboard")
    assert lineage is None


def test_extract_panel_lineage_uid_mapping_path(lineage_extractor, monkeypatch):
    panel = _make_panel(
        datasource={"type": "postgres", "uid": "postgres_uid", "name": "postgres-main"}
    )

    parsed_sql = MagicMock()
    parsed_sql.in_tables = ["urn:li:dataset:(postgres,test_db.public.test_table,PROD)"]
    parsed_sql.column_lineage = []

    expected_lineage = MagicMock()
    parse_sql_mock = MagicMock(return_value=parsed_sql)
    create_column_lineage_mock = MagicMock(return_value=expected_lineage)
    create_basic_lineage_mock = MagicMock()

    monkeypatch.setattr(lineage_extractor, "_parse_sql", parse_sql_mock)
    monkeypatch.setattr(
        lineage_extractor,
        "_create_column_lineage",
        create_column_lineage_mock,
    )
    monkeypatch.setattr(
        lineage_extractor,
        "_create_basic_lineage",
        create_basic_lineage_mock,
    )

    lineage = lineage_extractor.extract_panel_lineage(panel, "test-dashboard")

    assert lineage is expected_lineage
    parse_sql_mock.assert_called_once()
    assert (
        parse_sql_mock.call_args[0][1]
        is lineage_extractor.connection_map["postgres_uid"]
    )
    create_column_lineage_mock.assert_called_once()
    create_basic_lineage_mock.assert_not_called()


def test_extract_panel_lineage_name_mapping_fallback(
    mock_graph, mock_report, monkeypatch
):
    name_config = PlatformConnectionConfig(
        platform="postgres",
        database="name_db",
        database_schema="public",
    )
    extractor = LineageExtractor(
        platform="grafana",
        platform_instance="test-instance",
        env="PROD",
        connection_to_platform_map={"postgres-main": name_config},
        report=mock_report,
        graph=mock_graph,
    )
    panel = _make_panel(
        datasource={"type": "postgres", "uid": "missing_uid", "name": "postgres-main"}
    )

    monkeypatch.setattr(extractor, "_parse_sql", MagicMock(return_value=None))
    expected_lineage = MagicMock()
    create_basic_lineage_mock = MagicMock(return_value=expected_lineage)
    monkeypatch.setattr(extractor, "_create_basic_lineage", create_basic_lineage_mock)

    lineage = extractor.extract_panel_lineage(panel, "test-dashboard")

    assert lineage is expected_lineage
    create_basic_lineage_mock.assert_called_once()
    called_ds_uid, called_platform_config, _ = create_basic_lineage_mock.call_args[0]
    assert called_ds_uid == "missing_uid"
    assert called_platform_config is name_config


def test_extract_panel_lineage_type_mapping_fallback(
    mock_graph, mock_report, monkeypatch
):
    type_config = PlatformConnectionConfig(
        platform="postgres",
        database="type_db",
        database_schema="public",
    )
    extractor = LineageExtractor(
        platform="grafana",
        platform_instance="test-instance",
        env="PROD",
        connection_to_platform_map={"postgres": type_config},
        report=mock_report,
        graph=mock_graph,
    )
    panel = _make_panel(
        datasource={"type": "postgres", "uid": "missing_uid", "name": "unknown-name"}
    )

    monkeypatch.setattr(extractor, "_parse_sql", MagicMock(return_value=None))
    expected_lineage = MagicMock()
    create_basic_lineage_mock = MagicMock(return_value=expected_lineage)
    monkeypatch.setattr(extractor, "_create_basic_lineage", create_basic_lineage_mock)

    lineage = extractor.extract_panel_lineage(panel, "test-dashboard")

    assert lineage is expected_lineage
    create_basic_lineage_mock.assert_called_once()
    called_ds_uid, called_platform_config, _ = create_basic_lineage_mock.call_args[0]
    assert called_ds_uid == "missing_uid"
    assert called_platform_config is type_config


def test_extract_panel_lineage_table_lineage_fallback(lineage_extractor, monkeypatch):
    panel = _make_panel(datasource={"type": "postgres", "uid": "postgres_uid"})

    parsed_sql = MagicMock()
    parsed_sql.in_tables = [
        "urn:li:dataset:(postgres,test_db.public.table_a,PROD)",
        "urn:li:dataset:(postgres,test_db.public.table_b,PROD)",
    ]
    parsed_sql.column_lineage = []

    monkeypatch.setattr(
        lineage_extractor, "_parse_sql", MagicMock(return_value=parsed_sql)
    )
    monkeypatch.setattr(
        lineage_extractor,
        "_create_column_lineage",
        MagicMock(return_value=None),
    )

    lineage = lineage_extractor.extract_panel_lineage(panel, "test-dashboard")

    assert isinstance(lineage, MetadataChangeProposalWrapper)
    assert isinstance(lineage.aspect, UpstreamLineageClass)
    assert [u.dataset for u in lineage.aspect.upstreams] == parsed_sql.in_tables
    assert all(
        u.type == DatasetLineageTypeClass.TRANSFORMED for u in lineage.aspect.upstreams
    )


def test_extract_panel_lineage_basic_lineage_fallback(lineage_extractor, monkeypatch):
    panel = _make_panel(datasource={"type": "postgres", "uid": "postgres_uid"})

    monkeypatch.setattr(lineage_extractor, "_parse_sql", MagicMock(return_value=None))
    expected_lineage = MagicMock()
    create_basic_lineage_mock = MagicMock(return_value=expected_lineage)
    monkeypatch.setattr(
        lineage_extractor,
        "_create_basic_lineage",
        create_basic_lineage_mock,
    )

    lineage = lineage_extractor.extract_panel_lineage(panel, "test-dashboard")

    assert lineage is expected_lineage
    create_basic_lineage_mock.assert_called_once()


def test_extract_panel_lineage_legacy_build_dataset_urn_signature(
    lineage_extractor, monkeypatch
):
    panel = _make_panel(datasource={"type": "postgres", "uid": "postgres_uid"})

    def _legacy_build_dataset_urn(ds_type: str, ds_uid: str, panel_id: str) -> str:
        return f"urn:li:dataset:(grafana,{ds_type}.{ds_uid}.{panel_id},PROD)"

    monkeypatch.setattr(
        lineage_extractor,
        "_build_dataset_urn",
        _legacy_build_dataset_urn,
    )
    monkeypatch.setattr(lineage_extractor, "_parse_sql", MagicMock(return_value=None))
    expected_lineage = MagicMock()
    create_basic_lineage_mock = MagicMock(return_value=expected_lineage)
    monkeypatch.setattr(
        lineage_extractor,
        "_create_basic_lineage",
        create_basic_lineage_mock,
    )

    lineage = lineage_extractor.extract_panel_lineage(panel, "test-dashboard")

    assert lineage is expected_lineage
    create_basic_lineage_mock.assert_called_once()


def test_extract_raw_sql_prefers_most_sql_like_candidate(lineage_extractor):
    selected = lineage_extractor._extract_raw_sql(
        [
            {"expr": "rate(http_requests_total[5m])"},
            {"query": "SELECT 1"},
            {
                "sql": {
                    "query": (
                        "SELECT users.id FROM users "
                        "JOIN roles ON users.role_id = roles.id"
                    )
                }
            },
        ]
    )

    assert (
        selected == "SELECT users.id FROM users JOIN roles ON users.role_id = roles.id"
    )


def test_create_basic_lineage(lineage_extractor):
    ds_uid = "postgres_uid"
    ds_urn = make_dataset_urn_with_platform_instance(
        platform="grafana",
        name="test_dataset",
        platform_instance="test-instance",
        env="PROD",
    )

    platform_config = PlatformConnectionConfig(
        platform="postgres",
        database="test_db",
        database_schema="public",
    )

    lineage = lineage_extractor._create_basic_lineage(ds_uid, platform_config, ds_urn)

    assert isinstance(lineage, MetadataChangeProposalWrapper)
    assert isinstance(lineage.aspect, UpstreamLineageClass)
    assert len(lineage.aspect.upstreams) == 1


def test_create_column_lineage(lineage_extractor):
    mock_parsed_sql = MagicMock()
    mock_parsed_sql.in_tables = [
        "urn:li:dataset:(postgres,test_db.public.test_table,PROD)"
    ]
    mock_parsed_sql.column_lineage = [
        MagicMock(
            downstream=MagicMock(column="test_col"),
            upstreams=[
                MagicMock(
                    table="urn:li:dataset:(postgres,test_db.public.source_table,PROD)",
                    column="source_col",
                )
            ],
        )
    ]

    ds_urn = make_dataset_urn_with_platform_instance(
        platform="grafana",
        name="test_dataset",
        platform_instance="test-instance",
        env="PROD",
    )

    lineage = lineage_extractor._create_column_lineage(ds_urn, mock_parsed_sql)
    assert isinstance(lineage, MetadataChangeProposalWrapper)
    assert isinstance(lineage.aspect, UpstreamLineageClass)
    assert lineage.aspect.fineGrainedLineages is not None


def test_create_table_lineage(lineage_extractor):
    parsed_sql = MagicMock()
    parsed_sql.in_tables = [
        "urn:li:dataset:(postgres,test_db.public.table_a,PROD)",
        "urn:li:dataset:(postgres,test_db.public.table_b,PROD)",
    ]

    lineage = lineage_extractor._create_table_lineage(
        "urn:li:dataset:(grafana,test_dataset,PROD)", parsed_sql
    )

    assert isinstance(lineage, MetadataChangeProposalWrapper)
    assert isinstance(lineage.aspect, UpstreamLineageClass)
    assert len(lineage.aspect.upstreams) == 2
    assert all(
        upstream.type == DatasetLineageTypeClass.TRANSFORMED
        for upstream in lineage.aspect.upstreams
    )
