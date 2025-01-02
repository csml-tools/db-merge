from typing import Any, Callable, Optional

from sqlalchemy import Column, Table
from .source import TableSource


def check_same_columns(sources: list[TableSource]) -> bool:
    if len(sources) > 0:
        first = set(sources[0].table.columns.keys())
        for i in range(1, len(sources)):
            if set(sources[i].table.columns.keys()) != first:
                return False

    return True


def check_same_columns_raise(name: str, sources: list[TableSource]):
    if not check_same_columns(sources):
        raise RuntimeError(f"Overlapping {name} has non-matching columns")


type Rowdict = dict[tuple, dict]


def get_rows_by_primary_key(source: TableSource) -> Rowdict:
    pk_columns = source.table.primary_key.columns
    row_by_pk = {}

    for row in source.session.connection.execute(source.table.select()):
        rowdict = row._asdict()
        row_by_pk[tuple(rowdict[column.name] for column in pk_columns)] = rowdict

    return row_by_pk


def dicts_equal(
    d1: dict, d2: dict, *, eq: Callable[[Any, Any], bool] = lambda a, b: a == b
) -> bool:
    if len(d1) != len(d2):
        return False

    for key in d1:
        if key not in d2 or not eq(d1[key], d2[key]):
            return False

    return True


def check_same_data(sources: list[TableSource]):
    if len(sources) > 0:
        first = get_rows_by_primary_key(sources[0])
        for i in range(1, len(sources)):
            if not dicts_equal(
                get_rows_by_primary_key(sources[i]), first, eq=dicts_equal
            ):
                return False

    return True


def check_same_data_raise(name: str, sources: list[TableSource]):
    if not check_same_data(sources):
        raise RuntimeError(
            f"Overlapping {name} must have the same data across all sources"
        )


def single_primary_key(table: Table) -> Optional[Column[Any]]:
    primary_keys = table.primary_key.columns

    if len(primary_keys) > 0:
        if len(primary_keys) > 1:
            raise RuntimeError("Multi-column primary keys currently not supported")

        return primary_keys[0]
