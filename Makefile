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
	docker run --init -it --env-file .env -e 'CRON_SCHEDULE=* * * * *' openpodcast/connector-manager

.PHONY: run up
run up: docker-build ## run the dev stack using a mysql instance and the manager
	docker compose up

.PHONY: shell-%
shell-%: ## run a shell in the container
	docker compose exec $* bash

.PHONY: db-shell
db-shell: ## Opens the mysql shell inside the db container
	docker compose exec db bash -c 'mysql -uopenpodcast -popenpodcast openpodcast'

.PHONY: down
down: ## stop the dev stack and remove volumes
	docker compose down -v