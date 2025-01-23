import time

from sqlalchemy.exc import OperationalError


def upsert_method(
    table, conn, keys, data_iter, unique_keys=None, batch_size=1000, max_retries=5
):
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
    for i in range(0, len(data), batch_size):
        batch = data[i : i + batch_size]
        retries = 0
        while retries < max_retries:
            try:
                conn.execute(sql, batch)
                print(f"Batch of size {len(batch)} processed successfully.")
                break
            except OperationalError as e:
                if "deadlock" in str(e).lower():
                    retries += 1
                    print(f"Deadlock detected. Retrying {retries}/{max_retries}...")
                    time.sleep(2**retries)
                else:
                    raise e
