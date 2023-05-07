.PHONY: run dev
run dev:
	python3 -m job

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
	        -e ANCHOR_BASE_URL=https://podcasters.spotify.com/pod/api/proxy/v3