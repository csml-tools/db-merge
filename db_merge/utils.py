from typing import Any, Optional

from sqlalchemy import Column, Table

from .session import TableSource


def get_rows_by_primary_key(source: TableSource) -> dict[tuple, dict]:
    pk_columns = source.table.primary_key.columns
    row_by_pk = {}

    for row in source.session.connection.execute(source.table.select()):
        rowdict = row._asdict()
        row_by_pk[tuple(rowdict[column.name] for column in pk_columns)] = rowdict

    return row_by_pk


def single_primary_key(table: Table) -> Optional[Column[Any]]:
    primary_keys = table.primary_key.columns

    if len(primary_keys) > 0:
        if len(primary_keys) > 1:
            raise RuntimeError("Multi-column primary keys not currently supported")

        return primary_keys[0]


__all__ = [
    "single_primary_key",
    "get_rows_by_primary_key",
]
