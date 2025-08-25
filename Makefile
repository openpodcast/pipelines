SHELL := /bin/bash
.DEFAULT_GOAL := help

.PHONY: help
help: ## help message, list all command
	@echo -e "$$(grep -hE '^\S+.*:.*##' $(MAKEFILE_LIST) | sed -e 's/:.*##\s*/:/' -e 's/^\(.\+\):\(.*\)/\\x1b[36m\1\\x1b[m:\2/' | column -c2 -t -s :)"

.PHONY: docker-build
docker-build:
	docker build -t openpodcast/connector-manager .

.PHONY: docker-run
docker-run: 
	docker run --init -it --env-file .env openpodcast/connector-manager

.PHONY: run up dev
run up dev: docker-build ## run the dev stack with mysql and huey worker
	docker compose up

.PHONY: up-%
up-%: docker-build ## start a single service in the dev stack 
	docker compose up $*

.PHONY: shell-%
shell-%: ## run a shell in the container
	docker compose exec $* bash

.PHONY: db-shell
db-shell: ## Opens the mysql shell inside the db container
	docker compose exec db bash -c 'mysql -uopenpodcast -popenpodcast openpodcast'


.PHONY: logs
logs: ## view worker logs
	docker compose logs -f worker

.PHONY: down
down: ## stop the dev stack and remove volumes
	docker compose down -v