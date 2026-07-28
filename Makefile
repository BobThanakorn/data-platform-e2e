PYTHON ?= python
YEAR ?= 2024
MONTH ?= 1

.PHONY: bootstrap up down reset test lint validate ingest backfill bi observability lineage streaming spark recovery

bootstrap:
	docker compose build
	docker compose up airflow-init
	docker compose up -d
	docker compose run --rm superset-bootstrap

up:
	docker compose up -d

down:
	docker compose down

reset:
	docker compose down --volumes

test:
	$(PYTHON) -m pytest

lint:
	$(PYTHON) -m ruff check .

validate: test lint
	docker compose config --quiet
	docker compose exec airflow-scheduler airflow dags list-import-errors

ingest:
	docker compose exec airflow-scheduler airflow dags trigger nyc_taxi_medallion --conf '{"year":$(YEAR),"month":$(MONTH),"force_download":false}'

backfill:
	$(PYTHON) scripts/backfill.py --start 2024-01 --end 2024-12

bi:
	docker compose run --rm superset-bootstrap

observability:
	STATSD_ON=true docker compose --profile observability up -d

lineage:
	OPENLINEAGE_DISABLED=false docker compose --profile lineage up -d --force-recreate airflow-scheduler

streaming:
	docker compose --profile streaming up -d redpanda stream-consumer
	docker compose --profile streaming run --rm stream-replay

spark:
	docker compose --profile spark up -d spark-master spark-worker
	docker compose --profile spark run --rm spark-job

recovery:
	powershell -ExecutionPolicy Bypass -File scripts/recovery_drill.ps1 -Month 2024-01
