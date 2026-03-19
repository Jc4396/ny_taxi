import os
import subprocess


def get_env(name, default=None, required=False):
    value = os.getenv(name, default)
    if required and value is None:
        raise ValueError(f"Missing required env var: {name}")
    return value


COMMON_ARGS = [
    f"--pg-user={get_env('PG_USER', 'root')}",
    f"--pg-pass={get_env('PG_PASS', 'root')}",
    f"--pg-host={get_env('PG_HOST', 'pgdatabase')}",
    f"--pg-port={get_env('PG_PORT', '5432')}",
    f"--pg-db={get_env('PG_DB', 'ny_taxi')}",
]

subprocess.run([
    "uv", "run", "ingest_data.py",
    *COMMON_ARGS,
    "--target-table=green_taxi_trips_2025_11"
], check=True)

subprocess.run([
    "uv", "run", "ingest_zone.py",
    *COMMON_ARGS,
    "--target-table=zones"
], check=True)
