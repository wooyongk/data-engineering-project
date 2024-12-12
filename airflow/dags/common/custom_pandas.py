def upsert_method(table, conn, keys, data_iter, unique_keys=None):
    if unique_keys is None or not unique_keys:
        raise ValueError("At least one unique key must be specified for upsert.")

    columns = ", ".join([f"`{col}`" for col in keys])
    placeholders = ", ".join(["%s"] * len(keys))

    update_stmt = ", ".join(
        [f"`{col}` = VALUES(`{col}`)" for col in keys if col not in unique_keys]
    )

    sql = f"""
    INSERT INTO {table.name} ({columns})
    VALUES ({placeholders})
    ON DUPLICATE KEY UPDATE {update_stmt}
    """

    data = list(data_iter)
    conn.execute(sql, data)

    print(f"{len(data)} rows were processed (inserted or updated).")
