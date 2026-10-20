.PHONY: help install up migrate-source down logs status connectors test test-recovery benchmark trace config clean observability event-ui

PYTHON ?= python3
VENV := .venv/bin/python
CONNECT_URL ?= http://localhost:8083
TABLE ?= customers
BENCH_ROWS ?= 1000
TRACE_MAX_MESSAGES ?= 10
EVENT_UI_PORT ?= 8001

help:
	@printf '%s\n' \
	  'make install        Create the virtualenv and install Python dependencies' \
	  'make up             Start the stack and register/update both connectors' \
	  'make down           Stop the stack and retain persisted data' \
	  'make status         Show service and connector status' \
	  'make observability  Start metrics and dashboards (Prometheus/Grafana)' \
	  'make event-ui       Start the live source-event UI (EVENT_UI_PORT=8001)' \
	  'make test           Run all CDC end-to-end tests' \
	  'make test-recovery  Run the Connect restart/recovery test' \
	  'make benchmark      Measure batch replication (BENCH_ROWS=1000)' \
	  'make trace          Print Kafka events for TABLE (default: customers) with timestamps, keys, headers, offsets and payload' \
	  'make clean          Remove containers and local persisted data' \
	  'make migrate-source Add updated_at trigger for all source tables'

install:
	@if [ ! -x "$(VENV)" ]; then $(PYTHON) -m venv .venv; fi
	$(VENV) -m pip install -r requirements.txt

config:
	docker compose config --quiet

up: install config
	docker compose up -d --wait
	$(MAKE) migrate-source
	$(VENV) scripts/register_connectors.py --connect-url $(CONNECT_URL)

observability:
	docker compose up -d metrics-exporter prometheus grafana
	@printf '%s\n' 'Prometheus: http://localhost:9090' 'Grafana: http://localhost:3000 (admin/admin by default)' 'Metrics exporter: http://localhost:8000/metrics'

event-ui:
	docker compose up -d kafka
	@printf '%s\n' 'Event UI: http://localhost:$(EVENT_UI_PORT)'
	$(PYTHON) scripts/event_feed_server.py --listen-host 0.0.0.0 --port $(EVENT_UI_PORT)

migrate-source:
	docker compose exec -T postgres-source sh -c 'psql -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"' < source/updated_at.sql

down:
	docker compose down

logs:
	docker compose logs -f --timestamps

status:
	docker compose ps
	@curl -fsS $(CONNECT_URL)/connectors | $(PYTHON) -m json.tool
	@curl -fsS $(CONNECT_URL)/connectors/postgres-source/status | $(PYTHON) -m json.tool
	@curl -fsS $(CONNECT_URL)/connectors/sink-connector/status | $(PYTHON) -m json.tool

test: up
	@set -a; . ./.env; set +a; $(VENV) -m pytest -s -v tests/test_cdc_e2e.py

test-recovery: up
	@set -a; . ./.env; set +a; $(VENV) -m pytest -s -v tests/test_cdc_e2e.py -k recovery

benchmark: up
	@set -a; . ./.env; set +a; $(VENV) scripts/benchmark_cdc.py --rows $(BENCH_ROWS)

trace:
	@test -n "$(TABLE)" || (echo 'Set TABLE, for example: make trace TABLE=customers' && exit 1)
	docker exec kafka /kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server kafka:9092 \
		--topic cdc.public.$(TABLE) \
		--from-beginning \
		--max-messages $(TRACE_MAX_MESSAGES) \
		--timeout-ms 30000 \
		--formatter-property print.timestamp=true \
		--formatter-property print.key=true \
		--formatter-property print.headers=true \
		--formatter-property print.offset=true \
		--formatter-property print.partition=true \
		--formatter-property print.value=true

clean:
	docker compose down --volumes --remove-orphans
	rm -rf source_data target_data kafka_data pgadmin_data