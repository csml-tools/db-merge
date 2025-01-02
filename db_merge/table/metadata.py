from typing import TYPE_CHECKING, Iterator, Optional
from sqlalchemy import MetaData

if TYPE_CHECKING:
    from .classes import MergedTable


class MergedMetadata:
    def __init__(self, metadata: Optional[MetaData] = None) -> None:
        self.tables: dict[str, "MergedTable"] = {}
        self.sqlalchemy = metadata or MetaData()

    def add_table(self, key: str, table: "MergedTable"):
        self.tables[key] = table

    def sorted(self) -> Iterator["MergedTable"]:
        for table in self.sqlalchemy.sorted_tables:
            yield self.tables[table.key]
