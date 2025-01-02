from contextlib import ExitStack
from dataclasses import dataclass
import re
from typing import Annotated, Optional
from sqlalchemy import create_engine
import typer

from .reflect import InputSession
from .options import MergeOptions
from .click_params import PydanticFileLoader
from .merge import smart_merge


@dataclass
class SlicedUrl:
    slice: int
    url: str


def parse_sliced_url(value: str) -> SlicedUrl:
    if match := re.match(r"^(\d+)#(.+)$", value):
        return SlicedUrl(int(match.group(1)), match.group(2))
    else:
        raise ValueError("Expected format: SLICE#URL")


def main(
    inputs: Annotated[list[SlicedUrl], typer.Argument(parser=parse_sliced_url)],
    output: Annotated[str, typer.Option("-o", "--output")],
    options: Annotated[
        Optional[MergeOptions],
        typer.Option("-c", "--cfg", click_type=PydanticFileLoader(MergeOptions)),
    ] = None,
):
    with ExitStack() as stack:
        smart_merge(
            [
                InputSession.from_connection(
                    stack.enter_context(create_engine(input.url).connect()), input.slice
                )
                for input in inputs
            ],
            stack.enter_context(create_engine(output).connect()),
            options,
        )


typer.run(main)
