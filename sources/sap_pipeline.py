import dlt
from sap import sap, sap_bics, sap_odp


def load_sap_tables() -> None:
    """Loads SAP tables or CDS views via RFC."""
    pipeline = dlt.pipeline(
        pipeline_name="sap_tables",
        destination="duckdb",
        dataset_name="sap_data",
    )
    # Configure tables to replicate
    source = sap(tables=["MARA", "MAKT"])
    load_info = pipeline.run(source)
    print(load_info)


def load_sap_odp() -> None:
    """Loads SAP ODP (Operational Data Provisioning) data source."""
    pipeline = dlt.pipeline(
        pipeline_name="sap_odp",
        destination="duckdb",
        dataset_name="sap_odp_data",
    )
    # ODP delta extraction for a CDS view or DataSource
    source = sap_odp(
        context="ABAP_CDS",
        names=["Z_CDS_EXTRACT"],
    )
    load_info = pipeline.run(source)
    print(load_info)


def load_sap_bics() -> None:
    """Loads SAP BW queries via BICS."""
    pipeline = dlt.pipeline(
        pipeline_name="sap_bics",
        destination="duckdb",
        dataset_name="sap_bics_data",
    )
    # BICS analytical query extraction
    source = sap_bics(cube="0D_FC_SALES", queries=["0D_FC_SALES_Q001"])
    load_info = pipeline.run(source)
    print(load_info)


if __name__ == "__main__":
    load_sap_tables()
