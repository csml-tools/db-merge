from typing import Annotated, Optional
from pydantic import BaseModel, Field
from sqlalchemy import Table


class TableIdent(BaseModel, frozen=True):
    name: str
    db_schema: Annotated[Optional[str], Field(alias="schema")] = None

    @staticmethod
    def from_table(table: Table) -> "TableIdent":
        return TableIdent(name=table.name, db_schema=table.schema)

    def __str__(self) -> str:
        return f"{self.db_schema}.{self.name}" if self.db_schema else self.name


class SliceTable(TableIdent, frozen=True):
    slice_column: str


class MergeOptions(BaseModel):
    exclude: set[TableIdent] = set()
    same: list[TableIdent] = []
    sliced: list[SliceTable] = []
