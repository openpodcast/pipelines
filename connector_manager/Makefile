SHELL := /bin/bash
.DEFAULT_GOAL := help

.PHONY: help
help: ## help message, list all command
	@echo -e "$$(grep -hE '^\S+:.*##' $(MAKEFILE_LIST) | sed -e 's/:.*##\s*/:/' -e 's/^\(.\+\):\(.*\)/\\x1b[36m\1\\x1b[m:\2/' | column -c2 -t -s :)"

.PHONY: run dev
run dev: ## run the job
	. .venv/bin/activate && python3 -m manager

.PHONY: test
test: ## run the test and activate venv
	. .venv/bin/activate && pytest -v --cov=manager --cov-report=term-missing

.PHONY: install
install: ## init venv and install dependencies
	python3 -m venv .venv
	. .venv/bin/activate && pip install --upgrade pip && pip install -r requirements.txt
	