import json
import yaml
from pathlib import Path
from typing import Any, Optional
import click
from pydantic import BaseModel


class PydanticFileLoader[T: BaseModel](click.ParamType):
    def __init__(
        self,
        model: type[T],
    ) -> None:
        self.__model = model
        self.name = model.__name__

    def convert(
        self, value: Any, param: Optional[click.Parameter], ctx: Optional[click.Context]
    ) -> Any:
        def load_path(path: Path):
            with path.open() as file:
                if loader := {
                    ".json": json.load,
                    ".yaml": yaml.safe_load,
                    ".yml": yaml.safe_load,
                }.get(path.suffix, None):
                    return self.__model.model_validate(loader(file))
                else:
                    self.fail(f"Unknown file extension: {path}", param, ctx)

        if isinstance(value, self.__model):
            return value
        elif isinstance(value, str):
            return load_path(Path(value))
        elif isinstance(value, Path):
            return load_path(value)
        else:
            self.fail(f"Unexpected type: {type(value)}", param, ctx)
