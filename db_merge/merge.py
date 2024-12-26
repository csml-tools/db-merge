from contextlib import ExitStack
from dataclasses import dataclass
from typing import Optional
from sqlalchemy import create_engine
from sys import exit

from .options import MergeOptions, TableIdent
from .reflect import MergedGraph


@dataclass
class SlicedUrl:
    url: str
    slice: Optional[int] = None


def find_overlaps(graph: MergedGraph, options: MergeOptions) -> list[TableIdent]:
    # For tables with id_slice and their subtree, overlaps are allowed
    sliced_tree: set[TableIdent] = {
        merged_table.ident
        for merged_table in graph.sort(
            constraints=[table.ident() for table in options.sliced]
        )
    }

    return [
        merged_table.ident
        for merged_table in graph.tables.values()
        if len(merged_table.sources) > 1
        and merged_table.ident not in options.same
        and merged_table.ident not in sliced_tree
    ]


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
