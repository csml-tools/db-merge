from dataclasses import dataclass
from sqlalchemy import Table, Connection

from db_merge.reflect import InputSession


@dataclass()
class TableSource:
    table: Table
    session: InputSession

    @property
    def key(self) -> str:
        return self.table.key

    @property
    def connection(self) -> Connection:
        return self.session.connection
