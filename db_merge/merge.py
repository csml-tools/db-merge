from contextlib import ExitStack
from dataclasses import dataclass
from typing import Any, Callable, Optional
from sqlalchemy import create_engine, Connection
from sys import exit

from .options import MergeOptions, TableIdent
from .reflect import MergedGraph, MergedTable, TableSource


@dataclass
class SlicedUrl:
    url: str
    slice: Optional[int] = None


def find_overlaps(graph: MergedGraph, options: MergeOptions) -> list[TableIdent]:
    # For tables with id_slice and their subtree, overlaps are allowed
    sliced_tree: set[TableIdent] = {
        merged_table.ident
        for merged_table in graph.sort(
            constraints=[item.table for item in options.sliced]
        )
    }

    return [
        merged_table.ident
        for merged_table in graph.tables.values()
        if len(merged_table.sources) > 1
        and merged_table.ident not in options.same
        and merged_table.ident not in sliced_tree
    ]


def copy_single_table(source: TableSource, out_connection: Connection):
    source.table.create(out_connection, checkfirst=False)

    insert = source.table.insert()
    for row in source.connection.execute(source.table.select()):
        out_connection.execute(insert, row._asdict())


def check_same_columns(sources: list[TableSource]) -> bool:
    if len(sources) > 0:
        first = set(sources[0].table.columns.keys())
        for i in range(1, len(sources)):
            if set(sources[i].table.columns.keys()) != first:
                return False

    return True


def check_same_columns_raise(merged_table: MergedTable):
    if not check_same_columns(merged_table.sources):
        raise RuntimeError(f"Overlapping {merged_table.ident} has non-matching columns")


type Rowdict = dict[tuple, dict]


def get_rows_by_primary_key(source: TableSource) -> Rowdict:
    pk_columns = source.table.primary_key.columns
    row_by_pk = {}

    for row in source.connection.execute(source.table.select()):
        rowdict = row._asdict()
        row_by_pk[tuple(rowdict[column.name] for column in pk_columns)] = rowdict

    return row_by_pk


def dicts_equal(
    d1: dict, d2: dict, *, equality: Callable[[Any, Any], bool] = lambda a, b: a == b
) -> bool:
    if len(d1) != len(d2):
        return False

    for key in d1:
        if key not in d2 or not equality(d1[key], d2[key]):
            return False

    return True


def check_same_data(sources: list[TableSource]):
    if len(sources) > 0:
        first = get_rows_by_primary_key(sources[0])
        for i in range(1, len(sources)):
            if not dicts_equal(
                get_rows_by_primary_key(sources[i]), first, equality=dicts_equal
            ):
                return False

    return True


def check_same_data_raise(merged_table: MergedTable):
    if not check_same_data(merged_table.sources):
        raise RuntimeError(
            f"Overlapping {merged_table.ident} must have the same data across all sources"
        )


def smart_merge(
    inputs: list[SlicedUrl],
    output_url: str,
    options: Optional[MergeOptions] = None,
):
    options = options or MergeOptions()

    with ExitStack() as stack:
        graph = MergedGraph()
        for input in inputs:
            graph.add_database(
                stack.enter_context(create_engine(input.url).connect()),
                include=lambda table: table not in options.exclude,
            )

        if overlaps := find_overlaps(graph, options):
            print("Unsolved overlaps:")
            for overlapping in overlaps:
                print(overlapping)
            exit(1)

        out_connection = stack.enter_context(create_engine(output_url).connect())
        for merged_table in graph.sort():
            if len(merged_table.sources) == 1:
                copy_single_table(merged_table.sources[0], out_connection)
            else:
                check_same_columns_raise(merged_table)

                if merged_table.ident in options.same:
                    check_same_data_raise(merged_table)
                    copy_single_table(merged_table.sources[0], out_connection)
                else:
                    pass
