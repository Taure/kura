-module(kura_schema_verify_tests).

-include_lib("eunit/include/eunit.hrl").

-define(SCHEMA, kura_test_index_method_schema).

access_method_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        [
            {"declared methods match under any name", fun matches_declared_methods/0},
            {"a btree does not satisfy a declared hash index", fun reports_wrong_method/0},
            {"an index declared without using takes any method", fun any_method_without_using/0},
            {"a unique btree satisfies a plain declared btree", fun unique_btree_for_btree/0},
            {"a hash index does not satisfy a declared btree", fun hash_for_btree/0},
            {"unique and partial still match beside a method", fun unique_and_partial_match/0},
            {"a plain btree does not satisfy a declared unique one", fun plain_for_unique/0},
            {"a non-atom using is refused", fun refuses_non_atom_using/0}
        ]
    end}.

setup() ->
    application:ensure_all_started(minato),
    application:ensure_all_started(kura),
    kura_test_repo:start(),
    ok.

teardown(_) ->
    drop_table().

matches_declared_methods() ->
    create_table([
        ~"CREATE INDEX e ON kura_index_method_items USING hash (email)",
        ~"CREATE INDEX n ON kura_index_method_items (name)",
        ~"CREATE INDEX c ON kura_index_method_items USING btree (code)"
    ]),
    ?assertEqual(ok, kura_schema_verify:verify(kura_test_repo, [?SCHEMA])).

reports_wrong_method() ->
    create_table([
        ~"CREATE INDEX e ON kura_index_method_items (email)",
        ~"CREATE INDEX n ON kura_index_method_items (name)",
        ~"CREATE INDEX c ON kura_index_method_items (code)"
    ]),
    ?assertEqual(
        {drift, [{missing_index, ~"kura_index_method_items", [email], #{using => hash}}]},
        kura_schema_verify:verify(kura_test_repo, [?SCHEMA])
    ).

any_method_without_using() ->
    create_table([
        ~"CREATE INDEX e ON kura_index_method_items USING hash (email)",
        ~"CREATE INDEX n ON kura_index_method_items USING hash (name)",
        ~"CREATE INDEX c ON kura_index_method_items (code)"
    ]),
    ?assertEqual(ok, kura_schema_verify:verify(kura_test_repo, [?SCHEMA])).

unique_btree_for_btree() ->
    create_table([
        ~"CREATE INDEX e ON kura_index_method_items USING hash (email)",
        ~"CREATE INDEX n ON kura_index_method_items (name)",
        ~"CREATE UNIQUE INDEX c ON kura_index_method_items (code)"
    ]),
    ?assertEqual(ok, kura_schema_verify:verify(kura_test_repo, [?SCHEMA])).

hash_for_btree() ->
    create_table([
        ~"CREATE INDEX e ON kura_index_method_items USING hash (email)",
        ~"CREATE INDEX n ON kura_index_method_items (name)",
        ~"CREATE INDEX c ON kura_index_method_items USING hash (code)"
    ]),
    ?assertEqual(
        {drift, [{missing_index, ~"kura_index_method_items", [code], #{using => btree}}]},
        kura_schema_verify:verify(kura_test_repo, [?SCHEMA])
    ).

unique_and_partial_match() ->
    create_table([
        ~"CREATE UNIQUE INDEX e ON kura_index_method_items (email)",
        ~"CREATE INDEX n ON kura_index_method_items (name) WHERE name IS NOT NULL"
    ]),
    ?assertEqual(ok, kura_schema_verify:verify(kura_test_repo, [kura_test_index_shape_schema])).

plain_for_unique() ->
    create_table([
        ~"CREATE INDEX e ON kura_index_method_items (email)",
        ~"CREATE INDEX n ON kura_index_method_items (name)"
    ]),
    ?assertMatch(
        {drift, [{missing_index, _, [email], #{unique := true}}]},
        kura_schema_verify:verify(kura_test_repo, [kura_test_index_shape_schema])
    ).

create_table(Indexes) ->
    drop_table(),
    {ok, _} = kura_test_repo:query(
        ~"CREATE TABLE kura_index_method_items (id BIGSERIAL PRIMARY KEY, email VARCHAR(255), name VARCHAR(255), code VARCHAR(255))",
        []
    ),
    [{ok, _} = kura_test_repo:query(SQL, []) || SQL <- Indexes],
    ok.

refuses_non_atom_using() ->
    create_table([~"CREATE INDEX e ON kura_index_method_items USING hash (email)"]),
    ?assertError(
        {invalid_index_option, using, ~"hash"},
        kura_schema_verify:verify(kura_test_repo, [kura_test_bad_using_schema])
    ).

drop_table() ->
    {ok, _} = kura_test_repo:query(~"DROP TABLE IF EXISTS kura_index_method_items CASCADE", []),
    ok.
