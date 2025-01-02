from collections import defaultdict
from typing import Optional
from sqlalchemy import Connection

from .options import MergeOptions
from .table import (
    MergedMetadata,
    TableSource,
    SingleSourceTable,
    ReferenceTable,
    SlicedTable,
)
from .reflect import InputSession, reflect_metadata


def smart_merge(
    inputs: list[InputSession],
    output: Connection,
    options: Optional[MergeOptions] = None,
):
    options = options or MergeOptions()

    output_metadata = reflect_metadata(output)
    output_metadata.drop_all(output)

    table_sources: defaultdict[str, list[TableSource]] = defaultdict(list)
    for session in inputs:
        for table in session.metadata.tables.values():
            if table.key in options.exclude:
                continue

            table_sources[table.key].append(TableSource(table, session))

    # TODO: Check for unclassified overlaps
    merge_metadata = MergedMetadata(output_metadata)
    for key, sources in table_sources.items():
        if len(sources) == 1:
            merge_metadata.add_table(key, SingleSourceTable(sources[0], merge_metadata))
        elif key in options.same:
            merge_metadata.add_table(key, ReferenceTable(key, sources, merge_metadata))
        else:
            merge_metadata.add_table(key, SlicedTable(key, sources, merge_metadata))

    for table in merge_metadata.sorted():
        print(f"Creating {table._table.name}")
        table.create(output)
        table.insert(output)

    output.commit()
