from __future__ import annotations
from abc import ABC, abstractmethod
from collections import defaultdict
from contextlib import ExitStack
from typing import Callable, Concatenate, Iterator, Optional

from sqlalchemy import Connection, MetaData, Table, ForeignKey, create_engine, select
from sqlalchemy.sql.expression import func

from .options import MergeOptions
from .session import (
    SliceUrl,
    TableSource,
    TableSourceGroup,
    InputSession,
    reflect_metadata,
)
from .checks import check_same_data_raise, check_same_columns_raise, single_primary_key


class OverlayGraph:
    def __init__(self) -> None:
        self._tables: defaultdict[str, list[TableSource]] = defaultdict(list)
        self._relations: defaultdict[str, set[str]] = defaultdict(set)

    def add_table_source(self, source: TableSource):
        self._tables[source.table.key].append(source)

        for fk in source.table.foreign_keys:
            self._relations[fk.column.table.key].add(source.table.key)

    def iter_tables(self) -> Iterator[TableSourceGroup]:
        for key, sources in self._tables.items():
            yield TableSourceGroup(key, sources)

    def sort(self, constraints: Optional[list[str]] = None) -> list[TableSourceGroup]:
        stack: list[TableSourceGroup] = []
        visited: set[str] = set()

        def visit(key: str):
            if key not in self._tables:
                raise KeyError(f"Table {key} doesn't exist in graph")

            if key not in visited:
                visited.add(key)

                for child_key in self._relations[key]:
                    visit(child_key)

                stack.append(TableSourceGroup(key, self._tables[key]))

        for key in constraints if constraints is not None else self._tables:
            visit(key)

        stack.reverse()
        return stack


class OutputMetadata:
    def __init__(self, metadata: Optional[MetaData] = None) -> None:
        self.sqlalchemy = metadata or MetaData()
        self.tables: dict[str, OutputTable] = {}
        self.offsets: defaultdict[str, dict[int, int]] = defaultdict(dict)

    def add_table[
        **P
    ](
        self,
        constructor: Callable[Concatenate[OutputMetadata, P], OutputTable],
        *args: P.args,
        **kwargs: P.kwargs,
    ):
        table = constructor(self, *args, **kwargs)
        self.tables[table.key] = table

    def sorted_tables(self) -> Iterator[OutputTable]:
        for sqlalchemy_table in self.sqlalchemy.sorted_tables:
            yield self.tables[sqlalchemy_table.key]


class OutputTable(ABC):
    def __init__(self, metadata: OutputMetadata, table: Table) -> None:
        self._metadata = metadata
        self._table = table

    def create(self, connection: Connection):
        self._table.create(connection, checkfirst=False)
        self._insert(connection)

    @abstractmethod
    def _insert(self, connection: Connection): ...

    @property
    def key(self) -> str:
        return self._table.key


class SingleSourceTable(OutputTable):
    def __init__(self, metadata: OutputMetadata, source: TableSource) -> None:
        super().__init__(metadata, source.table.to_metadata(metadata.sqlalchemy))
        self._source = source

    def _insert(self, connection: Connection):
        insert_stmnt = self._table.insert()

        for row in self._source.connection.execute(self._source.table.select()):
            connection.execute(insert_stmnt, row._asdict())


class SliceTable(OutputTable):
    def __init__(self, metadata: OutputMetadata, sources: list[TableSource]) -> None:
        super().__init__(metadata, sources[0].table.to_metadata(metadata.sqlalchemy))
        self._primary_key = single_primary_key(self._table)
        self._sources = sorted(sources, key=lambda source: source.slice)

    def _insert(self, connection: Connection):
        # Insert without the primary key
        insert_stmnt = self._table.insert().values(
            {
                column.name: None
                for column in self._table.columns.values()
                if column is not self._primary_key
            }
        )

        offsetted_foreign_keys: list[tuple[str, ForeignKey]] = []
        for column in self._table.columns.values():
            for fk in column.foreign_keys:
                if (
                    fk.column.primary_key
                    and fk.column.table.key in self._metadata.offsets
                ):
                    offsetted_foreign_keys.append((column.name, fk))

        for source in self._sources:
            if self._primary_key is not None:
                offset = (
                    connection.execute(select(func.max(self._primary_key))).scalar()
                    or 0
                )
                self._metadata.offsets[self.key][source.slice] = offset

            for row in source.connection.execute(source.table.select()):
                rowdict = row._asdict()
                if self._primary_key is not None:
                    del rowdict[self._primary_key.name]

                for column_name, fk in offsetted_foreign_keys:
                    if rowdict[column_name] is not None:
                        rowdict[column_name] += self._metadata.offsets[
                            fk.column.table.key
                        ][source.slice]

                connection.execute(insert_stmnt, rowdict)


def smart_merge(
    input_urls: list[SliceUrl],
    output_url: str,
    options: Optional[MergeOptions] = None,
):
    options = options or MergeOptions()

    with ExitStack() as stack:
        inputs = [stack.enter_context(InputSession.connect(url)) for url in input_urls]

        graph = OverlayGraph()
        for session in inputs:
            for table in session.metadata.tables.values():
                if table.key not in options.exclude:
                    graph.add_table_source(TableSource(table, session))

        # Child tables of a slice table are also considered slice tables
        slice_tables = {
            group.key for group in graph.sort([item.table for item in options.sliced])
        }

        out_metadata = OutputMetadata()
        for group in graph.iter_tables():
            if len(group.sources) == 1:
                out_metadata.add_table(SingleSourceTable, group.sources[0])
            elif len(group.sources) > 1:
                if group.key in options.same:
                    check_same_columns_raise(group)
                    check_same_data_raise(group)
                    out_metadata.add_table(SingleSourceTable, group.sources[0])
                elif group.key in slice_tables:
                    check_same_columns_raise(group)
                    out_metadata.add_table(SliceTable, group.sources)
                else:
                    raise RuntimeError(
                        f"Couldn't classify overlapping table {group.key}"
                    )

        with create_engine(output_url).connect() as out_connection:
            reflect_metadata(out_connection).drop_all(out_connection)

            for table in out_metadata.sorted_tables():
                print("Creating", table.key)
                table.create(out_connection)

            out_connection.commit()
