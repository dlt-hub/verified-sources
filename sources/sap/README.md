# SAP

SAP ERP, S/4HANA, and SAP BW are enterprise systems holding core business transactions, master data, and analytical data.

Using this `dlt` verified source and pipeline example, you can extract tables, CDS views, BW queries (BICS), and ODP delta subscriptions from SAP directly into any [destination](https://dlthub.com/docs/dlt-ecosystem/destinations/) of your choice.

Data extraction is powered by the [erpl-dlt](https://github.com/DataZooDE/erpl-dlt) library and DuckDB extensions, providing native streaming Arrow record batches and exact SAP type conversions (`DATS` as `date32`, `CURR` as `decimal128`, packed decimals, and null handling).

| Source Name | Description |
|---|---|
| `sap` / `sap_table` | Extracts SAP transparent tables and ABAP Core Data Services (CDS) views via `RFC_READ_TABLE` / `TABLE_READ_DYNAMIC` with cursor-based pagination |
| `sap_odp` | Operational Data Provisioning (ODP) extractor supporting initial full load and incremental delta queues |
| `sap_bics` | Business Intelligence Consumer Services (BICS) interface for SAP BW InfoProviders and BEx queries |
| `sap_odata` | Direct SAP OData service extractor |
| `sap_function` | Executes RFC function modules and BAPIs |

## Initialize the pipeline

```bash
dlt init sap duckdb
```

Here we chose `duckdb` as the destination. Alternatively, you can choose `bigquery`, `snowflake`, `postgres`, `redshift`, or any other [destination](https://dlthub.com/docs/dlt-ecosystem/destinations/).

## Prerequisites & SAP RFC SDK

The SAP source connects to SAP via standard Remote Function Calls (RFC).
- Python dependencies are automatically installed from PyPI: `pip install -r requirements.txt`.
- For environments running the RFC protocol, the SAP NetWeaver RFC SDK (`libsapnwrfc.so`) must be accessible on your system's library path (`LD_LIBRARY_PATH`), or preloaded into container images.
- A valid SAP user with authorization for RFC execution and table reads (e.g. `S_RFC`, `S_TABU_DIS`).

## Add credentials

1. Open `.dlt/secrets.toml`.

2. Add your SAP RFC credentials:

   ```toml
   # Direct application server connection:
   [sources.sap.credentials]
   ashost = "sap-appserver.company.com"
   sysnr = "00"
   client = "100"
   user = "DATA_LOADER"
   password = "SecretPassword"

   # Or load-balanced message server connection:
   # mshost = "sap-msgserver.company.com"
   # sysid = "PRD"
   # group = "PUBLIC"
   # client = "100"
   # user = "DATA_LOADER"
   # password = "SecretPassword"
   ```

3. Optionally, configure tables in `.dlt/config.toml`:

   ```toml
   [sources.sap]
   tables = ["MARA", "MAKT", "VBAK"]
   ```

4. Configure destination credentials according to the [destination documentation](https://dlthub.com/docs/dlt-ecosystem/destinations/).

## Run the pipeline example

1. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

2. Run the example pipeline:

   ```bash
   python sap_pipeline.py
   ```
