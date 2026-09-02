.PHONY: help up down logs migrate seed test test-int lint fmt typecheck loadtest reset

help:
	@echo "up        - start postgres, redis, api and workers"
	@echo "down      - stop everything"
	@echo "logs      - tail api + worker logs"
	@echo "migrate   - apply database migrations"
	@echo "seed      - generate demo contacts (COUNT=200000)"
	@echo "test      - unit tests"
	@echo "test-int  - unit + integration tests (needs postgres/redis)"
	@echo "lint      - ruff"
	@echo "typecheck - mypy"
	@echo "loadtest  - run the load test (ACCOUNT=<uuid>)"

up:
	docker compose up -d --build
	@echo "API on http://localhost:8000/docs"

down:
	docker compose down

reset:
	docker compose down -v

logs:
	docker compose logs -f api worker

migrate:
	docker compose exec api alembic upgrade head

COUNT ?= 200000
DUP ?= 0.0
seed:
	docker compose exec api python scripts/seed.py --count $(COUNT) --duplicate-ratio $(DUP)

test:
	pytest tests/unit -q

test-int:
	pytest -q

lint:
	ruff check app tests scripts

fmt:
	ruff check --fix app tests scripts

typecheck:
	mypy app

ACCOUNT ?=
loadtest:
	python scripts/load_test.py --account-id $(ACCOUNT)
