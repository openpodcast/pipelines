.PHONY: run dev
run dev:
	python3 -m job

.PHONY: install
install:
	pip3 install -r requirements.txt

.PHONY: docker-build
docker-build:
	docker build -t openpodcast/spotify-connector .

.PHONY: docker-run
docker-run:
	docker run --init -it --env-file .env -e 'CRON_SCHEDULE=* * * * *' openpodcast/spotify-connector

.env.sample:
	@envsync -e OPENPODCAST_API_ENDPOINT=https://api.openpodcast.dev \
	        -e SPOTIFY_BASE_URL=https://generic.wg.spotify.com/podcasters/v0 \
			-e SPOTIFY_CLIENT_ID=05a1371ee5194c27860b3ff3ff3979d2 \
			-e STORE_DATA=False	