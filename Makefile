# Everything runs in the container: the harness targets the task's Linux image and nothing else.
.PHONY: build test crap demo shell clean

build:
	docker compose build

test: build
	docker compose run --rm harness test

crap: build
	docker compose run --rm harness crap

demo: build
	docker compose run --rm harness demo

shell: build
	docker compose run --rm harness shell

clean:
	docker compose down -v --rmi local
