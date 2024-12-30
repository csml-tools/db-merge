from __future__ import annotations
from dataclasses import dataclass, field
from collections import defaultdict
from typing import Callable, Optional
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


@dataclass
class MergedTable:
    ident: TableIdent
    sources: list[TableSource] = field(default_factory=list)


class MergedGraph:
    def __init__(self) -> None:
        self.tables: dict[TableIdent, MergedTable] = {}
        self.inverse_relations: defaultdict[TableIdent, set[TableIdent]] = defaultdict(
            set
        )

    def add_database(
        self, connection: Connection, include: Callable[[Table], bool] | None = None
    ):
        for table in reflect_metadata(connection).tables.values():
            if not include or include(table):

                ident = TableIdent.from_table(table)
                if ident not in self.tables:
                    self.tables[ident] = MergedTable(ident)

                self.tables[ident].sources.append(TableSource(connection, table))

                for fk in table.foreign_key_constraints:
                    self.inverse_relations[
                        TableIdent.from_table(fk.referred_table)
                    ].add(ident)

    def sort(self, constraints: Optional[list[TableIdent]] = None) -> list[MergedTable]:
        stack: list[MergedTable] = []
        visited: set[TableIdent] = set()

        def visit(ident: TableIdent):
            if ident not in visited:
                visited.add(ident)

                for referer in self.inverse_relations[ident]:
                    visit(referer)

                stack.append(self.tables[ident])

        for ident in self.tables if constraints is None else constraints:
            visit(ident)

        stack.reverse()
        return stack
