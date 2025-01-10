.PHONY: run dev
run dev:
	set -a && source ./.env && python3 -m job

.PHONY: test
test:
	python3 -m pytest

.PHONY: install
install:
	pipenv run pip3 install -r requirements.txt

.env.sample:
	@envsync -e OPENPODCAST_API_ENDPOINT=https://api.openpodcast.dev \
	        -e ANCHOR_BASE_URL=https://podcasters.spotify.com/pod/api/proxy/v3