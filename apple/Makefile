.PHONY: help
help: ## list all targets
	@grep -hE '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

.PHONY: install
install: ## install dependencies (incl. dev) into a uv-managed venv
	uv sync --dev

.PHONY: run dev
run dev: ## run the apple pipeline locally using values from ./.env
	set -a && source ./.env && uv run python -m job

.PHONY: test
test: ## run the test suite
	uv run pytest -v

.PHONY: lint
lint: ## run ruff lint and format checks
	uv run ruff check .
	uv run ruff format --check .

.PHONY: format
format: ## auto-format with ruff
	uv run ruff format .

.PHONY: lock
lock: ## refresh uv.lock without installing
	uv lock

.env.sample:
	envsync