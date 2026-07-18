.PHONY: build up down replay test

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

replay:
	./scripts/replay.sh $(PCAP) $(PPS)

test:
	uv run pytest
