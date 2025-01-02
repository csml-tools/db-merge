from contextlib import contextmanager
from dataclasses import dataclass
from sqlalchemy import Engine, Connection, MetaData


def reflect_metadata(source: Engine | Connection) -> MetaData:
    metadata = MetaData()
    metadata.reflect(source)
    return metadata


@dataclass
class InputSession:
    connection: Connection
    metadata: MetaData
    slice: int

    @staticmethod
    def from_connection(connection: Connection, slice: int):
        return InputSession(connection, reflect_metadata(connection), slice)
