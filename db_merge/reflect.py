from __future__ import annotations
from dataclasses import dataclass
from collections import defaultdict
from typing import Callable
from sqlalchemy import Engine, Connection, MetaData, Table

from .options import TableIdent


def reflect_metadata(source: Engine | Connection) -> MetaData:
    metadata = MetaData()
    metadata.reflect(source)
    return metadata


@dataclass
class TableSource:
    connection: Connection
    table: Table


class MergedTable:
    def __init__(self) -> None:
        self.sources: list[TableSource] = []

    @property
    def ident(self) -> TableIdent:
        return TableIdent.from_table(self.sources[0].table)


class MergedGraph:
    def __init__(self) -> None:
        self.tables: defaultdict[TableIdent, MergedTable] = defaultdict(MergedTable)
        self.inverse_relations: defaultdict[TableIdent, set[TableIdent]] = defaultdict(
            set
        )

    def add_database(
        self, connection: Connection, include: Callable[[Table], bool] | None = None
    ):
        for table in reflect_metadata(connection).tables.values():
            if not include or include(table):

                ident = TableIdent.from_table(table)
                self.tables[ident].sources.append(TableSource(connection, table))

                for fk in table.foreign_key_constraints:
                    self.inverse_relations[
                        TableIdent.from_table(fk.referred_table)
                    ].add(ident)

    def sort(self) -> list[MergedTable]:
        stack: list[MergedTable] = []
        visited: set[TableIdent] = set()

        def visit(ident: TableIdent):
            if ident not in visited:
                visited.add(ident)

                for referer in self.inverse_relations[ident]:
                    visit(referer)

                stack.append(self.tables[ident])

        for ident in self.tables:
            visit(ident)

        stack.reverse()
        return stack
