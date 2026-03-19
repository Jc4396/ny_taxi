# Build docke taxi ingest image

docker build -t taxi_ingest:v001 .

# Now run the script

docker run -it --rm
--network=pipeline_default
taxi_ingest:v001
-e PG_USER=root
-e PG_PASS=root
-e PG_HOST=pgdatabase
-e PG_PORT=5432
-e PG_DB=ny_taxi