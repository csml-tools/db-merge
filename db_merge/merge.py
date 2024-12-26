from contextlib import ExitStack
from dataclasses import dataclass
from typing import Optional
from sqlalchemy import create_engine

from .options import MergeOptions
from .reflect import MergedGraph


@dataclass
class SlicedUrl:
    url: str
    slice: Optional[int] = None


def smart_merge(
    inputs: list[SlicedUrl],
    # output_url: str,
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

        for merged_table in graph.sort():
            print(len(merged_table.sources), merged_table.ident)
