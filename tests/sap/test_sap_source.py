from unittest import mock
import pyarrow as pa
import pytest

import dlt
from sources.sap import (
    SapRfcCredentials,
    sap,
    sap_bics,
    sap_odp,
)


@pytest.fixture
def mock_rfc_credentials() -> SapRfcCredentials:
    return SapRfcCredentials(
        ashost="sap.test.local",
        sysnr="00",
        client="100",
        user="TEST_USER",
        password="TestPassword123",
    )


def test_sap_source_resource_discovery(mock_rfc_credentials: SapRfcCredentials) -> None:
    source = sap(tables=["MARA", "MAKT"], credentials=mock_rfc_credentials)
    resource_names = list(source.resources.keys())
    assert "mara" in resource_names
    assert "makt" in resource_names


def test_sap_source_credentials_validation() -> None:
    with pytest.raises(ValueError, match="ashost.*mshost"):
        creds = SapRfcCredentials(client="100", user="TEST")
        creds.on_resolved()


def test_sap_odp_source_definition(mock_rfc_credentials: SapRfcCredentials) -> None:
    source = sap_odp(
        context="ABAP_CDS",
        names=["Z_CDS_VIEW"],
        credentials=mock_rfc_credentials,
    )
    assert "abap_cds_z_cds_view" in source.resources


def test_sap_bics_source_definition(mock_rfc_credentials: SapRfcCredentials) -> None:
    source = sap_bics(
        cube="0D_FC_SALES",
        queries=["0D_FC_SALES_Q001"],
        credentials=mock_rfc_credentials,
    )
    assert "0d_fc_sales_q001" in source.resources


def test_sap_source_mock_execution(mock_rfc_credentials: SapRfcCredentials) -> None:
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array(["MAT001", "MAT002"]),
            pa.array(["FERT", "ROH"]),
        ],
        names=["MATNR", "MTART"],
    )

    pipeline = dlt.pipeline(
        pipeline_name="test_sap_tables",
        destination="duckdb",
        dataset_name="test_sap_data",
    )

    with mock.patch("erpl_dlt.rfc._batches", return_value=iter([batch])):
        source = sap(tables=["MARA"], credentials=mock_rfc_credentials)
        load_info = pipeline.run(source)

    assert load_info.has_failed_jobs is False

    with pipeline.sql_client() as client:
        with client.execute_query(
            "SELECT matnr, mtart FROM mara ORDER BY matnr"
        ) as cur:
            rows = cur.fetchall()
            assert len(rows) == 2
            assert rows[0] == ("MAT001", "FERT")
            assert rows[1] == ("MAT002", "ROH")
