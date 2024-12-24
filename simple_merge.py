from typing import Annotated
import typer
from contextlib import ExitStack, contextmanager
from sqlalchemy import (
    create_engine,
    Connection,
    Engine,
    MetaData,
)


def reflect_metadata(source: Engine | Connection) -> MetaData:
    metadata = MetaData()
    metadata.reflect(source)
    return metadata


class SmartConnection:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection
        self.metadata = reflect_metadata(connection)

    @contextmanager
    @staticmethod
    def connect(engine: Engine):
        with engine.connect() as conn:
            yield SmartConnection(conn)


def simple_merge(inputs: list[SmartConnection], output: Connection):
    for input in inputs:
        for table in input.metadata.sorted_tables:
            print("Inserting", table.key)
            table.create(output, checkfirst=False)  # throw error if already exists

            insert = table.insert()
            for row in input.connection.execute(table.select()):
                output.execute(insert, row._asdict())


def main(
    input_urls: list[str], output_url: Annotated[str, typer.Option("-o", "--output")]
):
    with ExitStack() as stack:
        inputs = [
            stack.enter_context(SmartConnection.connect(create_engine(url)))
            for url in input_urls
        ]
        output = stack.enter_context(SmartConnection.connect(create_engine(output_url)))

        output.metadata.drop_all(output.connection)
        simple_merge(inputs, output.connection)
        output.connection.commit()


if __name__ == "__main__":
    typer.run(main)
