import re
import typer
from typing import Annotated, Optional

from .options import MergeOptions
from .click_params import PydanticFileLoader
from .merge import smart_merge
from .session import SliceUrl


def parse_sliced_url(value: str) -> SliceUrl:
    if match := re.match(r"^(\d+)#(.+)$", value):
        return SliceUrl(int(match.group(1)), match.group(2))
    else:
        raise ValueError("Expected format: SLICE#URL")


def main(
    inputs: Annotated[list[SliceUrl], typer.Argument(parser=parse_sliced_url)],
    output: Annotated[str, typer.Option("-o", "--output")],
    options: Annotated[
        Optional[MergeOptions],
        typer.Option("-c", "--cfg", click_type=PydanticFileLoader(MergeOptions)),
    ] = None,
):
    smart_merge(inputs, output, options)


typer.run(main)
