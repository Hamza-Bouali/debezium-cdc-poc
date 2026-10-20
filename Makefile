.PHONY: help install up down logs status connectors test test-recovery benchmark trace config clean

PYTHON ?= python3
VENV := .venv/bin/python
CONNECT_URL ?= http://localhost:8083
TABLE ?= customers
BENCH_ROWS ?= 1000

help:
	@printf '%s\n' \
	  'make install       Create the virtualenv and install Python dependencies' \
	  'make up            Start the stack and register/update both connectors' \
	  'make down          Stop the stack and retain persisted data' \
	  'make status        Show service and connector status' \
	  'make test          Run all CDC end-to-end tests' \
	  'make test-recovery Run the Connect restart/recovery test' \
	  'make benchmark     Measure batch replication (BENCH_ROWS=1000)' \
	  'make trace         Print Kafka events for TABLE (default: customers)' \
	  'make clean         Remove containers and local persisted data'

install:
	@if [ ! -x "$(VENV)" ]; then $(PYTHON) -m venv .venv; fi
	$(VENV) -m pip install -r requirements.txt

config:
	docker compose config --quiet

up: install config
	docker compose up -d --wait
	$(VENV) scripts/register_connectors.py --connect-url $(CONNECT_URL)

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
	docker exec kafka /kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic cdc.public.$(TABLE) --from-beginning --max-messages 10 --timeout-ms 30000 --property print.timestamp=true --property print.key=true

clean:
	docker compose down --volumes --remove-orphans
	rm -rf source_data target_data kafka_data pgadmin_data