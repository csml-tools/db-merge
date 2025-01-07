import re
import typer
from dataclasses import dataclass
from typing import Annotated, Optional

from .options import MergeOptions
from .click_params import PydanticFileLoader
from .merge import smart_merge
from .session import SliceUrl


@dataclass
class OptionalSliceUrl:
    slice: Optional[int]
    url: str

    @staticmethod
    def parse(value: str):
        if match := re.match(r"^(\d+)#(.+)$", value):
            return OptionalSliceUrl(int(match.group(1)), match.group(2))
        else:
            return OptionalSliceUrl(None, value)


def main(
    inputs: Annotated[
        list[OptionalSliceUrl],
        typer.Argument(
            parser=OptionalSliceUrl.parse,
            help="inputs sqlalchemy URLs, slice can be specified with a prefix, like 5#",
        ),
    ],
    output: Annotated[
        str, typer.Option("-o", "--output", help="output sqlalchemy URL")
    ],
    options: Annotated[
        Optional[MergeOptions],
        typer.Option(
            "-c",
            "--cfg",
            click_type=PydanticFileLoader(MergeOptions),
            help="JSON/YAML config file",
        ),
    ] = None,
):
    inputs_sliced = [
        SliceUrl(input.slice if input.slice is not None else i, input.url)
        for i, input in enumerate(inputs)
    ]

    smart_merge(inputs_sliced, output, options)


typer.run(main)
