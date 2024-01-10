.PHONY: run dev
run dev:
	set -a && source ./.env && python3 -m job

.PHONY: test
test:
	python3 -m pytest

.PHONY: install
install:
	pip3 install -r requirements.txt

.PHONY: docker-build
docker-build:
	docker build -t openpodcast/podigee-connector .

.PHONY: docker-run
docker-run:
	docker run --init -it --env-file .env -e 'CRON_SCHEDULE=* * * * *' openpodcast/podigee-connector

.env.sample:
	@envsync -e OPENPODCAST_API_ENDPOINT=https://api.openpodcast.dev \
	        -e PODIGEE_BASE_URL="https://app.podigee.com/api/v1"