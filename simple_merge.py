from typing import Annotated
import typer
from contextlib import ExitStack
from sqlalchemy import (
    create_engine,
    Connection,
    Engine,
    MetaData,
    URL,
    make_url,
)


def reflect_metadata(source: Engine | Connection) -> MetaData:
    metadata = MetaData()
    metadata.reflect(source)
    return metadata


def simple_merge(inputs: list[Connection], output: Connection):
    for input_connection in inputs:
        metadata = reflect_metadata(input_connection)

        for table in metadata.sorted_tables:
            print("Inserting", table.key)
            table.create(output, checkfirst=False)  # throw error if already exists

            insert = table.insert()
            for row in input_connection.execute(table.select()):
                output.execute(insert, row._asdict())


def main(
    input_urls: list[str],
    output_url: Annotated[URL, typer.Option("-o", "--output", parser=make_url)],
):
    with ExitStack() as stack:
        input_connections = [
            stack.enter_context(create_engine(url).connect()) for url in input_urls
        ]
        output_connection = stack.enter_context(create_engine(output_url).connect())

        reflect_metadata(output_connection).drop_all(output_connection)
        simple_merge(input_connections, output_connection)
        output_connection.commit()


if __name__ == "__main__":
    typer.run(main)
