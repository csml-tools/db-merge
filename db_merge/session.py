from contextlib import contextmanager
from dataclasses import dataclass
from sqlalchemy import Engine, Connection, MetaData, Table, create_engine


def reflect_metadata(source: Engine | Connection) -> MetaData:
    metadata = MetaData()
    metadata.reflect(source)
    return metadata


@dataclass
class SliceUrl:
    slice: int
    url: str


@dataclass
class InputSession:
    slice: int
    connection: Connection
    metadata: MetaData

    @contextmanager
    @staticmethod
    def connect(input: SliceUrl):
        with create_engine(input.url).connect() as connection:
            yield InputSession(input.slice, connection, reflect_metadata(connection))


@dataclass
class TableSource:
    table: Table
    session: InputSession

    @property
    def connection(self) -> Connection:
        return self.session.connection

    @property
    def metadata(self) -> MetaData:
        return self.session.metadata

    @property
    def slice(self) -> int:
        return self.session.slice


@dataclass
class TableSourceGroup:
    key: str
    sources: list[TableSource]
