from typing import Optional
from pydantic import BaseModel


class SliceTable(BaseModel):
    table: str
    slice_column: Optional[str] = None


class MergeOptions(BaseModel):
    exclude: set[str] = set()
    same: set[str] = set()
    sliced: list[SliceTable] = []
