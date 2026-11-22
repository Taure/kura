-module(kura_schema_verify_opclass_tests).

-include_lib("eunit/include/eunit.hrl").

-define(SCHEMA, kura_test_index_opclass_schema).
-define(TABLE, ~"kura_index_opclass_items").

opclass_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        [
            {"a declared opclass matches the live one", fun matches_declared_opclass/0},
            {"another opclass does not satisfy a declared one", fun reports_wrong_opclass/0},
            {"an index declared without opclass takes any", fun any_opclass_without_opclass/0},
            {"an opclass over several columns is refused", fun refuses_multi_column/0}
        ]
    end}.

setup() ->
    application:ensure_all_started(minato),
    application:ensure_all_started(kura),
    kura_test_repo:start(),
    {ok, _} = kura_test_repo:query(~"CREATE EXTENSION IF NOT EXISTS vector", []),
    ok.

teardown(_) ->
    drop_table().

matches_declared_opclass() ->
    create_table([
        ~"CREATE INDEX e ON kura_index_opclass_items USING hnsw (embedding vector_cosine_ops)",
        ~"CREATE INDEX c ON kura_index_opclass_items (code)"
    ]),
    ?assertEqual(ok, kura_schema_verify:verify(kura_test_repo, [?SCHEMA])).

reports_wrong_opclass() ->
    create_table([
        ~"CREATE INDEX e ON kura_index_opclass_items USING hnsw (embedding vector_l2_ops)",
        ~"CREATE INDEX c ON kura_index_opclass_items (code)"
    ]),
    ?assertEqual(
        {drift, [
            {missing_index, ?TABLE, [embedding], #{using => hnsw, opclass => vector_cosine_ops}}
        ]},
        kura_schema_verify:verify(kura_test_repo, [?SCHEMA])
    ).

any_opclass_without_opclass() ->
    create_table([
        ~"CREATE INDEX e ON kura_index_opclass_items USING hnsw (embedding vector_cosine_ops)",
        ~"CREATE INDEX c ON kura_index_opclass_items (code text_pattern_ops)"
    ]),
    ?assertEqual(ok, kura_schema_verify:verify(kura_test_repo, [?SCHEMA])).

refuses_multi_column() ->
    create_table([~"CREATE INDEX c ON kura_index_opclass_items (code, id)"]),
    ?assertError(
        {invalid_index_option, opclass, {columns, [code, id]}},
        kura_schema_verify:verify(kura_test_repo, [kura_test_multi_opclass_schema])
    ).

create_table(Indexes) ->
    drop_table(),
    {ok, _} = kura_test_repo:query(
        ~"CREATE TABLE kura_index_opclass_items (id BIGSERIAL PRIMARY KEY, embedding vector(3), code VARCHAR(255))",
        []
    ),
    [{ok, _} = kura_test_repo:query(SQL, []) || SQL <- Indexes],
    ok.

drop_table() ->
    {ok, _} = kura_test_repo:query(~"DROP TABLE IF EXISTS kura_index_opclass_items CASCADE", []),
    ok.
