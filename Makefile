.PHONY: test test-up test-down _test compile fmt check

DOCKER_COMPOSE = docker compose

test: test-up _test test-down

_test:
	rebar3 eunit || ($(MAKE) test-down && exit 1)

test-up:
	$(DOCKER_COMPOSE) up -d
	@echo "Waiting for Postgres..."
	@until docker exec $$($(DOCKER_COMPOSE) ps -q postgres) pg_isready -U postgres > /dev/null 2>&1; do sleep 0.5; done
	@echo "Postgres ready."

test-down:
	$(DOCKER_COMPOSE) down

compile:
	rebar3 compile

fmt:
	rebar3 fmt

check:
	rebar3 fmt --check
	rebar3 xref
	rebar3 dialyzer
