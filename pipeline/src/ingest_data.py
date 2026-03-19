import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

# NOTE: These are the defaults used when no CLI args are provided.
DEFAULT_PG_USER = "root"
DEFAULT_PG_PASS = "root"
DEFAULT_PG_HOST = "localhost"
DEFAULT_PG_PORT = "5432"
DEFAULT_PG_DB = "ny_taxi"
DEFAULT_TARGET_TABLE = "zones"
# DEFAULT_CHUNKSIZE = 100000


@click.command()
@click.option("--pg-user",
              default=DEFAULT_PG_USER,
              show_default=True,
              help="Postgres username")
@click.option("--pg-pass",
              default=DEFAULT_PG_PASS,
              show_default=True,
              help="Postgres password")
@click.option("--pg-host",
              default=DEFAULT_PG_HOST,
              show_default=True,
              help="Postgres host")
@click.option("--pg-port",
              default=DEFAULT_PG_PORT,
              show_default=True,
              help="Postgres port")
@click.option("--pg-db",
              default=DEFAULT_PG_DB,
              show_default=True,
              help="Postgres database name")
@click.option("--target-table",
              default=DEFAULT_TARGET_TABLE,
              show_default=True,
              help="Target table name in Postgres")
# @click.option("--chunksize",
# default=DEFAULT_CHUNKSIZE,
# show_default=True,
# type=int,
# help="Number of rows to ingest per chunk")
def run(
    pg_user,
    pg_pass,
    pg_host,
    pg_port,
    pg_db,
    target_table,
    # chunksize,
):
    """Ingest NYC taxi zone data into Postgres."""

    # Set the URL for the dataset
    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

    engine = create_engine(
        f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    )

    df = pd.read_csv(url)

    # Create table schema
    df.head(0).to_sql(
        name=target_table,
        con=engine,
        if_exists="replace",
    )
    print("Table created")

    # Insert data with progress bar
    with tqdm(total=len(df), desc="Inserting rows") as pbar:
        df.to_sql(
            name=target_table,
            con=engine,
            if_exists="append",
            method="multi",
        )
        pbar.update(len(df))
    print("Data inserted successfully")


if __name__ == "__main__":
    run()
