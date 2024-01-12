def add_columns(columns, rows):
    """Add column names to the given rows.

    Args:
        columns (List[str]): A list of column names.
        rows (List[Tuple[Any]]): A list of rows.

    Returns:
        List[Dict[str, Any]]:
            A list of rows with column names.
    """
    result = []
    for row in rows:
        result.append(dict(zip(columns, row)))

    return result
