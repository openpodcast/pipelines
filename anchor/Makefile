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
	docker build -t openpodcast/anchor-connector .

.PHONY: docker-run
docker-run:
	docker run --init -it --env-file .env -e 'CRON_SCHEDULE=* * * * *' openpodcast/anchor-connector

.env.sample:
	@envsync -e OPENPODCAST_API_ENDPOINT=https://api.openpodcast.dev \
	        -e SPOTIFY_SP_DC= \
	        -e SPOTIFY_SP_KEY=