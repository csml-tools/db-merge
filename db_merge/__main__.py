import re
from typing import Annotated, Optional
import typer

from .options import MergeOptions
from .click_params import PydanticFileLoader
from .merge import SlicedUrl, smart_merge


def parse_sliced_url(value: str) -> SlicedUrl:
    if match := re.match(r"^(.+)#(\d+)$", value):
        return SlicedUrl(match.group(1), int(match.group(2)))
    else:
        return SlicedUrl(value)


def main(
    inputs: Annotated[list[SlicedUrl], typer.Argument(parser=parse_sliced_url)],
    output: Annotated[str, typer.Option("-o", "--output")],
    options: Annotated[
        Optional[MergeOptions],
        typer.Option("-c", "--cfg", click_type=PydanticFileLoader(MergeOptions)),
    ] = None,
):
    smart_merge(inputs, output, options)


typer.run(main)
