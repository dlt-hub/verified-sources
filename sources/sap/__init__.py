"""dlt verified source for SAP systems (ERP, S/4HANA, BW).

Extracts data from SAP via RFC (Tables & CDS views), BICS (BW queries),
ODP (Operational Data Provisioning delta sync), and RFC function modules
using the ERPL DuckDB extensions and erpl-dlt.
"""

from collections.abc import Iterator, Mapping, Sequence
from typing import Any, Optional, cast

import dlt
from dlt.sources import DltResource

from erpl_dlt import (
    ErplConnection,
    ErplSettings,
    ODataCredentials,
    SapRfcCredentials,
    erpl_bics_source,
    erpl_invoke_source,
    erpl_odata_source,
    erpl_odp_odata_source,
    erpl_odp_source,
    erpl_rfc_source,
)


@dlt.source(name="sap")
def sap(
    tables: Sequence[str] = dlt.config.value,
    credentials: Optional[SapRfcCredentials] = dlt.secrets.value,
    *,
    columns: Optional[Mapping[str, Sequence[str]]] = None,
    abap_filters: Optional[Mapping[str, str]] = None,
    cursor_columns: Optional[Mapping[str, str]] = None,
    primary_keys: Optional[Mapping[str, Sequence[str]]] = None,
    max_rows: Optional[int] = None,
    write_disposition: Optional[str] = None,
    settings: Optional[ErplSettings] = None,
    connection: Any = None,
) -> Iterator[DltResource]:
    """Source that extracts SAP tables and CDS views via RFC.

    Args:
        tables: Sequence of SAP table or CDS view names (e.g. ["MARA", "MAKT", "SFLIGHT"]).
        credentials: SAP RFC logon credentials (ashost/sysnr or mshost/sysid/group).
        columns: Optional column subsets per table: {"MARA": ["MATNR", "MTART"]}.
        abap_filters: Optional ABAP WHERE conditions per table: {"MARA": "MTART = 'FERT'"}.
        cursor_columns: Optional watermark columns for incremental sync: {"SFLIGHT": "FLDATE"}.
        primary_keys: Optional primary keys per table.
        max_rows: Optional row ceiling for sampling.
        write_disposition: Default write disposition ("replace", "append", or "merge").
        settings: Optional ERPL runtime engine settings.
        connection: Optional pre-configured ErplConnection.
    """
    return cast(
        Iterator[DltResource],
        erpl_rfc_source(
            table_names=tables,
            credentials=credentials,
            columns=columns,
            abap_filters=abap_filters,
            cursor_columns=cursor_columns,
            primary_keys=primary_keys,
            max_rows=max_rows,
            write_disposition=write_disposition,
            settings=settings,
            connection=connection,
        ),
    )


# Source aliases and specialized protocols
sap_source = sap
sap_table = sap
sap_rfc = sap
sap_bics = erpl_bics_source
sap_odp = erpl_odp_source
sap_odp_odata = erpl_odp_odata_source
sap_odata = erpl_odata_source
sap_function = erpl_invoke_source

__all__ = [
    "ErplConnection",
    "ErplSettings",
    "ODataCredentials",
    "SapRfcCredentials",
    "sap",
    "sap_bics",
    "sap_function",
    "sap_odata",
    "sap_odp",
    "sap_odp_odata",
    "sap_rfc",
    "sap_source",
    "sap_table",
]
