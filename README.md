# airflow-exporter

airflow exporter for prometheus monitoring

## build

~~~~
docker login
docker-compose -f docker-compose-build.yml build
docker-compose -f docker-compose-build.yml push
~~~~

## configuration

Set appropriate environment variables, see docker-compose.yml

## run

Use docker-compose.yml to run container with mounted config exporter/exporter.py.yml
~~~~
docker-compose up
~~~~

