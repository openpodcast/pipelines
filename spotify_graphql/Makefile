.PHONY: help run dev install test

help:
	@grep -hE '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?##"}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

run dev:  ## Run the pipeline (auto-loads .env if present)
	@[ -f .env ] && . ./.env; python3 -m job

install:  ## Install dependencies into a local venv
	python3 -m venv .venv
	.venv/bin/pip install -r requirements.txt

test:  ## Run tests
	python3 -m pytest