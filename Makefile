.PHONY: test test-up test-down _test compile fmt check \
       test-stress test-bench test-bench-full test-migration-stress test-resilience test-production

DOCKER_COMPOSE = docker compose

test: test-up _test test-down

EXISTING_TESTS = kura_assoc_tests,kura_cast_assoc_tests,kura_changeset_tests,\
	kura_embed_tests,kura_integration_assoc_tests,kura_integration_tests,\
	kura_many_to_many_tests,kura_migrator_tests,kura_multi_tests,\
	kura_query_compiler_tests,kura_query_tests,kura_schemaless_tests,\
	kura_telemetry_tests,kura_types_tests

_test:
	rebar3 eunit --module=$(EXISTING_TESTS) || ($(MAKE) test-down && exit 1)

test-up:
	$(DOCKER_COMPOSE) up -d
	@echo "Waiting for Postgres..."
	@until docker exec $$($(DOCKER_COMPOSE) ps -q postgres) pg_isready -U postgres > /dev/null 2>&1; do sleep 0.5; done
	@echo "Postgres ready."

test-down:
	$(DOCKER_COMPOSE) down

test-stress: test-up
	rebar3 eunit --module=kura_stress_tests || ($(MAKE) test-down && exit 1)
	$(MAKE) test-down

test-bench: test-up
	rebar3 eunit --module=kura_bench_tests || ($(MAKE) test-down && exit 1)
	$(MAKE) test-down

test-bench-full: test-up
	rebar3 eunit --module=kura_comprehensive_bench_tests || ($(MAKE) test-down && exit 1)
	$(MAKE) test-down

test-migration-stress: test-up
	rebar3 eunit --module=kura_migration_stress_tests || ($(MAKE) test-down && exit 1)
	$(MAKE) test-down

test-resilience: test-up
	rebar3 eunit --module=kura_resilience_tests || ($(MAKE) test-down && exit 1)
	$(MAKE) test-down

test-production: test-up
	rebar3 eunit --module=kura_stress_tests,kura_bench_tests,kura_migration_stress_tests || ($(MAKE) test-down && exit 1)
	rebar3 eunit --module=kura_resilience_tests || ($(MAKE) test-down && exit 1)
	$(MAKE) test-down

compile:
	rebar3 compile

fmt:
	rebar3 fmt

check:
	rebar3 fmt --check
	rebar3 xref
	rebar3 dialyzer
