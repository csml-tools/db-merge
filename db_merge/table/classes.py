from abc import ABC, abstractmethod
from typing import Any
from sqlalchemy import (
    Connection,
    Table,
    Column,
    Integer,
    insert,
    select,
    bindparam,
)

from .metadata import MergedMetadata
from .source import TableSource
from .checks import check_same_columns_raise, check_same_data_raise, single_primary_key


class MergedTable(ABC):
    def __init__(self, table: Table, metadata: MergedMetadata) -> None:
        self._table = table
        self._metadata = metadata

        metadata.add_table(table.key, self)

    def create(self, connection: Connection):
        self._table.create(connection, checkfirst=False)

    @abstractmethod
    def insert(self, output: Connection): ...

    def remap_field(self, target_column: Column, old_value: Any) -> Any:
        return old_value


class SingleSourceTable(MergedTable):
    def __init__(self, source: TableSource, metadata: MergedMetadata) -> None:
        super().__init__(source.table.to_metadata(metadata.sqlalchemy), metadata)
        self._source = source

    def insert(self, output: Connection):
        insert_sql = self._table.insert()

        for row in self._source.connection.execute(self._source.table.select()):
            output.execute(insert_sql, row._asdict())


class ReferenceTable(SingleSourceTable):
    def __init__(
        self, key: str, sources: list[TableSource], metadata: MergedMetadata
    ) -> None:
        check_same_columns_raise(key, sources)
        check_same_data_raise(key, sources)

        super().__init__(sources[0], metadata)


class SlicedTable(MergedTable):
    def __init__(
        self, key: str, sources: list[TableSource], metadata: MergedMetadata
    ) -> None:
        check_same_columns_raise(key, sources)

        new_table = sources[0].table.to_metadata(metadata.sqlalchemy)
        new_table.append_column(Column("__dbmerge_slice", Integer))

        self._primary_key = single_primary_key(new_table)
        if self._primary_key is not None:
            new_table.append_column(Column("__dbmerge_old_id", self._primary_key.type))

        self._sources = sources
        super().__init__(new_table, metadata)

    def remap_field(self, target_column: Column, old_value: Any) -> Any:
        if (
            self._primary_key is not None
            and target_column.name == self._primary_key.name
        ):
            return (
                select(target_column)
                .where(
                    self._table.c["__dbmerge_old_id"]
                    == old_value & self._table.c["__dbmerge_slice"]
                    == bindparam("__dbmerge_slice")
                )
                .scalar_subquery()
            )
        return old_value

    def insert(self, output: Connection):
        insert_fields: list[tuple[str, Any]] = []
        if self._primary_key is not None:
            insert_fields.append(
                ("__dbmerge_old_id", bindparam(self._primary_key.name))
            )
        insert_fields.append(("__dbmerge_slice", bindparam("__dbmerge_slice")))

        for column in self._table.columns:
            if column.primary_key or column.name.startswith("__dbmerge_"):
                continue

            value = bindparam(column.name)

            if len(column.foreign_keys) == 1:
                fk = next(iter(column.foreign_keys))
                value = self._metadata.tables[fk.column.table.key].remap_field(
                    fk.column, value
                )
            elif len(column.foreign_keys) > 1:
                print(
                    f"{column} has multiple foreign keys, id remapping currently not supported for such columns"
                )

            insert_fields.append((column.name, value))

        insert_statement = insert(self._table).from_select(
            [name for name, _ in insert_fields],
            select(*(value for _, value in insert_fields)),
        )
        for source in self._sources:
            for row in source.connection.execute(source.table.select()):
                output.execute(
                    insert_statement,
                    {**row._mapping, "__dbmerge_slice": source.session.slice},
                )
