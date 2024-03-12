def add_pk(sql_client, table_name: str, column_name: str) -> None:
    """Adds primary key to postgres table.

    In the context of replication, the primary key serves as REPLICA IDENTITY.
    A REPLICA IDENTITY is required when publishing UPDATEs and/or DELETEs.
    """
    with sql_client() as c:
        qual_name = c.make_qualified_table_name(table_name)
        c.execute_sql(f"ALTER TABLE {qual_name} ADD PRIMARY KEY ({column_name});")
