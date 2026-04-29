.PHONY: run dev
run dev:
	set -a && source ./.env && python3 -m job

.PHONY: test
test:
	python3 -m pytest

.PHONY: install
install:
	pip3 install -r requirements.txt

.env.sample:
	envsync