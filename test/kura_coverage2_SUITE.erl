-module(kura_coverage2_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("kura.hrl").

-compile(export_all).

-eqwalizer({nowarn_function, migrator_quote_atom/1}).
-eqwalizer({nowarn_function, dump_embed_one_nested/1}).
-eqwalizer({nowarn_function, load_embed_one_nested/1}).

%%----------------------------------------------------------------------
%% CT callbacks
%%----------------------------------------------------------------------

all() ->
    [
        {group, repo_worker_crud},
        {group, repo_worker_errors},
        {group, preloader_edges},
        {group, stream_edges},
        {group, migrator_edges},
        {group, query_compiler_edges},
        {group, schema_edges},
        {group, sandbox_edges},
        {group, changeset_edges},
        {group, types_edges}
    ].

groups() ->
    [
        {repo_worker_crud, [sequence], [
            insert_with_opts_empty_rows,
            insert_with_opts_pg_error,
            update_record_not_found,
            delete_record_fk_error,
            insert_with_assoc_fk_error,
            update_with_assoc_not_found,
            persist_owned_assoc_has_one,
            check_constraint_violation,
            query_error_returns_error,
            all_error_bad_table,
            get_error_bad_table,
            get_by_error_bad_table,
            one_error_bad_table,
            exists_error_bad_table,
            update_all_error_bad_table,
            delete_all_error_bad_table,
            insert_all_error_bad_table,
            insert_all_opts_error_bad_table,
            insert_all_opts_returning_error_bad_table,
            delete_not_found
        ]},
        {repo_worker_errors, [], [
            build_log_event_rows_key,
            right_full_join_queries
        ]},
        {preloader_edges, [], [
            preload_belongs_to_all_nil_fks,
            preload_nested_nil_stays_nil
        ]},
        {stream_edges, [sequence], [
            stream_with_batch_size_1
        ]},
        {migrator_edges, [], [
            migrator_quote_atom,
            migrator_compile_index_5_arity,
            migrator_compile_index_where_proplist
        ]},
        {query_compiler_edges, [], [
            on_conflict_constraint_replace_all,
            on_conflict_constraint_replace
        ]},
        {schema_edges, [], [
            schema_no_primary_key,
            schema_primary_key_field_undefined,
            schema_column_map_explicit_column
        ]},
        {sandbox_edges, [], [
            sandbox_checkin_no_conn,
            sandbox_allowed_lookup_miss
        ]},
        {changeset_edges, [], [
            is_blank_null,
            build_schema_constraints_check,
            cast_assoc_existing_child_no_pk
        ]},
        {types_edges, [], [
            cast_enum_int_error,
            cast_array_non_list_error,
            dump_embed_one_nested,
            load_embed_one_nested,
            format_uuid_passthrough
        ]}
    ].

%%----------------------------------------------------------------------
%% Suite init/end
%%----------------------------------------------------------------------

init_per_suite(Config) ->
    application:ensure_all_started(pgo),
    application:ensure_all_started(kura),
    kura_test_repo:start(),
    kura_sandbox:start(),
    create_tables(),
    Config.

end_per_suite(_Config) ->
    drop_tables(),
    ok.

%%----------------------------------------------------------------------
%% Group init/end
%%----------------------------------------------------------------------

init_per_group(stream_edges, Config) ->
    Config;
init_per_group(_, Config) ->
    Config.

end_per_group(stream_edges, _Config) ->
    kura_test_repo:query("DELETE FROM users WHERE name LIKE 'C2S_%'", []),
    ok;
end_per_group(_, _Config) ->
    ok.

%%----------------------------------------------------------------------
%% Testcase init/end
%%----------------------------------------------------------------------

no_sandbox_tests() ->
    [
        migrator_quote_atom,
        migrator_compile_index_5_arity,
        migrator_compile_index_where_proplist,
        schema_no_primary_key,
        schema_primary_key_field_undefined,
        schema_column_map_explicit_column,
        sandbox_checkin_no_conn,
        sandbox_allowed_lookup_miss,
        is_blank_null,
        build_schema_constraints_check,
        cast_assoc_existing_child_no_pk,
        cast_enum_int_error,
        cast_array_non_list_error,
        dump_embed_one_nested,
        load_embed_one_nested,
        format_uuid_passthrough,
        build_log_event_rows_key,
        stream_with_batch_size_1
    ].

init_per_testcase(TC, Config) ->
    case lists:member(TC, no_sandbox_tests()) of
        true ->
            Config;
        false ->
            kura_sandbox:checkout(kura_test_repo),
            Config
    end.

end_per_testcase(TC, _Config) ->
    case lists:member(TC, no_sandbox_tests()) of
        true ->
            ok;
        false ->
            kura_sandbox:checkin(kura_test_repo),
            ok
    end.

%%----------------------------------------------------------------------
%% Table management
%%----------------------------------------------------------------------

create_tables() ->
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE IF NOT EXISTS users ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  name VARCHAR(255) NOT NULL,"
        "  email VARCHAR(255) NOT NULL,"
        "  age INTEGER,"
        "  active BOOLEAN DEFAULT true,"
        "  role VARCHAR(255) DEFAULT 'user',"
        "  score DOUBLE PRECISION,"
        "  metadata JSONB,"
        "  tags TEXT[],"
        "  lock_version INTEGER DEFAULT 0,"
        "  inserted_at TIMESTAMPTZ,"
        "  updated_at TIMESTAMPTZ"
        ")",
        []
    ),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE IF NOT EXISTS profiles ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  bio VARCHAR(255),"
        "  user_id BIGINT NOT NULL REFERENCES users(id),"
        "  inserted_at TIMESTAMPTZ,"
        "  updated_at TIMESTAMPTZ"
        ")",
        []
    ),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE IF NOT EXISTS posts ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  title VARCHAR(255) NOT NULL,"
        "  body TEXT,"
        "  author_id BIGINT NOT NULL REFERENCES users(id),"
        "  inserted_at TIMESTAMPTZ,"
        "  updated_at TIMESTAMPTZ"
        ")",
        []
    ),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE IF NOT EXISTS comments ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  body TEXT NOT NULL,"
        "  post_id BIGINT NOT NULL REFERENCES posts(id),"
        "  author_id BIGINT NOT NULL REFERENCES users(id),"
        "  inserted_at TIMESTAMPTZ,"
        "  updated_at TIMESTAMPTZ"
        ")",
        []
    ),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE IF NOT EXISTS tags ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  name VARCHAR(255) NOT NULL,"
        "  inserted_at TIMESTAMPTZ,"
        "  updated_at TIMESTAMPTZ"
        ")",
        []
    ),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE IF NOT EXISTS posts_tags ("
        "  post_id BIGINT NOT NULL REFERENCES posts(id),"
        "  tag_id BIGINT NOT NULL REFERENCES tags(id),"
        "  PRIMARY KEY (post_id, tag_id)"
        ")",
        []
    ),
    ok.

drop_tables() ->
    kura_test_repo:query("DROP TABLE IF EXISTS posts_tags CASCADE", []),
    kura_test_repo:query("DROP TABLE IF EXISTS comments CASCADE", []),
    kura_test_repo:query("DROP TABLE IF EXISTS tags CASCADE", []),
    kura_test_repo:query("DROP TABLE IF EXISTS posts CASCADE", []),
    kura_test_repo:query("DROP TABLE IF EXISTS profiles CASCADE", []),
    kura_test_repo:query("DROP TABLE IF EXISTS users CASCADE", []),
    ok.

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

insert_user(Name, Email) ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{name => Name, email => Email}, [name, email]),
    CS1 = kura_changeset:validate_required(CS, [name, email]),
    kura_test_repo:insert(CS1).

insert_post(Title, Body, AuthorId) ->
    CS = kura_changeset:cast(
        kura_test_post_schema,
        #{},
        #{title => Title, body => Body, author_id => AuthorId},
        [title, body, author_id]
    ),
    kura_test_repo:insert(CS).

%%======================================================================
%% repo_worker_crud: DB-hitting tests covering error and edge paths
%%======================================================================

%% Covers kura_repo_worker line 175-176: insert/3 with on_conflict returning empty rows
insert_with_opts_empty_rows(_Config) ->
    kura_test_repo:query("CREATE UNIQUE INDEX IF NOT EXISTS cov2_email_uniq ON users (email)", []),
    {ok, _} = insert_user(<<"C2ER">>, <<"c2er@test.com">>),
    CS = kura_changeset:cast(
        kura_test_schema,
        #{},
        #{name => <<"C2ER2">>, email => <<"c2er@test.com">>},
        [name, email]
    ),
    {ok, Applied} = kura_repo_worker:insert(kura_test_repo, CS, #{on_conflict => {email, nothing}}),
    ?assert(is_map(Applied)),
    kura_test_repo:query("DROP INDEX IF EXISTS cov2_email_uniq", []).

%% Covers kura_repo_worker line 178: insert/3 with PG error
insert_with_opts_pg_error(_Config) ->
    CS = kura_changeset:cast(
        kura_test_schema,
        #{},
        #{email => <<"c2pe@test.com">>},
        [email]
    ),
    {error, ErrCS} = kura_repo_worker:insert(kura_test_repo, CS, #{}),
    ?assertNot(ErrCS#kura_changeset.valid),
    ?assertEqual(insert, ErrCS#kura_changeset.action).

%% Covers kura_repo_worker line 463-468: update where record not found
update_record_not_found(_Config) ->
    CS = kura_changeset:cast(
        kura_test_schema,
        #{id => 999888777},
        #{name => <<"Ghost">>},
        [name]
    ),
    {error, ErrCS} = kura_test_repo:update(CS),
    ?assertNot(ErrCS#kura_changeset.valid),
    ?assert(lists:keymember(base, 1, ErrCS#kura_changeset.errors)).

%% Covers kura_repo_worker line 656: delete returns {error, _} from PG (FK constraint)
delete_record_fk_error(_Config) ->
    {ok, User} = insert_user(<<"C2Del">>, <<"c2del@test.com">>),
    UserId = maps:get(id, User),
    {ok, _Post} = insert_post(<<"C2Del Post">>, <<"body">>, UserId),
    CS = kura_changeset:cast(kura_test_schema, User, #{}, []),
    Result = kura_test_repo:delete(CS),
    ?assertMatch({error, _}, Result).

%% Covers kura_repo_worker line 157: insert with assoc_changes where insert_record fails
insert_with_assoc_fk_error(_Config) ->
    CS = kura_changeset:cast(
        kura_test_post_schema,
        #{},
        #{
            title => <<"AErr">>,
            body => <<"b">>,
            author_id => 999999,
            comments => [#{body => <<"c">>, author_id => 1}]
        },
        [title, body, author_id]
    ),
    CS1 = kura_changeset:cast_assoc(CS, comments),
    Result = kura_test_repo:insert(CS1),
    ?assertMatch({error, _}, Result).

%% Covers kura_repo_worker line 191: update with assoc_changes where update fails
update_with_assoc_not_found(_Config) ->
    CS = kura_changeset:cast(
        kura_test_post_schema,
        #{id => 999888777},
        #{
            title => <<"U">>,
            comments => [#{body => <<"c">>, author_id => 1}]
        },
        [title]
    ),
    CS1 = kura_changeset:cast_assoc(CS, comments),
    Result = kura_test_repo:update(CS1),
    ?assertMatch({error, _}, Result).

%% Covers kura_repo_worker lines 574-579: persist_owned_assoc with single changeset (has_one)
persist_owned_assoc_has_one(_Config) ->
    {ok, User} = insert_user(<<"C2HO">>, <<"c2ho@test.com">>),
    CS = kura_changeset:cast(
        kura_test_schema,
        User,
        #{profile => #{bio => <<"HO bio">>}},
        []
    ),
    CS1 = kura_changeset:cast_assoc(CS, profile),
    {ok, Updated} = kura_test_repo:update(CS1),
    Profile = maps:get(profile, Updated),
    ?assert(is_map(Profile)),
    ?assertEqual(<<"HO bio">>, maps:get(bio, Profile)),
    ?assertEqual(maps:get(id, User), maps:get(user_id, Profile)).

%% Covers kura_repo_worker line 731-737: check constraint violation (code 23514)
check_constraint_violation(_Config) ->
    kura_test_repo:query("ALTER TABLE users ADD CONSTRAINT cov2_age_chk CHECK (age > 0)", []),
    CS = kura_changeset:cast(
        kura_test_schema,
        #{},
        #{name => <<"C2Chk">>, email => <<"c2chk@test.com">>, age => -1},
        [name, email, age]
    ),
    CS1 = kura_changeset:check_constraint(CS, <<"cov2_age_chk">>, age),
    {error, ErrCS} = kura_test_repo:insert(CS1),
    ?assertNot(ErrCS#kura_changeset.valid),
    kura_test_repo:query("ALTER TABLE users DROP CONSTRAINT IF EXISTS cov2_age_chk", []).

%% Covers kura_repo_worker line 305: query returning error
query_error_returns_error(_Config) ->
    {error, _} = kura_test_repo:query("INVALID SQL GARBAGE!!!", []).

%% Covers kura_repo_worker line 86: all/2 error from bad table
all_error_bad_table(_Config) ->
    Q = kura_query:from(kura_cov2_bad_table_schema),
    {error, _} = kura_repo_worker:all(kura_test_repo, Q).

%% Covers kura_repo_worker line 97: get/3 error propagation
get_error_bad_table(_Config) ->
    {error, _} = kura_repo_worker:get(kura_test_repo, kura_cov2_bad_table_schema, 1).

%% Covers kura_repo_worker line 115: get_by/3 error propagation
get_by_error_bad_table(_Config) ->
    {error, _} = kura_repo_worker:get_by(kura_test_repo, kura_cov2_bad_table_schema, [
        {name, <<"x">>}
    ]).

%% Covers kura_repo_worker line 125: one/2 error propagation
one_error_bad_table(_Config) ->
    Q = kura_query:from(kura_cov2_bad_table_schema),
    {error, _} = kura_repo_worker:one(kura_test_repo, Q).

%% Covers kura_repo_worker line 135: exists/2 error propagation
exists_error_bad_table(_Config) ->
    Q = kura_query:from(kura_cov2_bad_table_schema),
    {error, _} = kura_repo_worker:exists(kura_test_repo, Q).

%% Covers kura_repo_worker line 209: update_all/3 error
update_all_error_bad_table(_Config) ->
    Q = kura_query:from(kura_cov2_bad_table_schema),
    {error, _} = kura_repo_worker:update_all(kura_test_repo, Q, #{name => <<"x">>}).

%% Covers kura_repo_worker line 219: delete_all/2 error
delete_all_error_bad_table(_Config) ->
    Q = kura_query:from(kura_cov2_bad_table_schema),
    {error, _} = kura_repo_worker:delete_all(kura_test_repo, Q).

%% Covers kura_repo_worker line 239: insert_all/3 error
insert_all_error_bad_table(_Config) ->
    Entries = [#{name => <<"x">>}],
    {error, _} = kura_repo_worker:insert_all(kura_test_repo, kura_cov2_bad_table_schema, Entries).

%% Covers kura_repo_worker line 270: insert_all/4 error without returning
insert_all_opts_error_bad_table(_Config) ->
    Entries = [#{name => <<"x">>}],
    {error, _} = kura_repo_worker:insert_all(
        kura_test_repo, kura_cov2_bad_table_schema, Entries, #{}
    ).

%% Covers kura_repo_worker line 265: insert_all/4 error with returning
insert_all_opts_returning_error_bad_table(_Config) ->
    Entries = [#{name => <<"x">>}],
    {error, _} = kura_repo_worker:insert_all(
        kura_test_repo, kura_cov2_bad_table_schema, Entries, #{returning => true}
    ).

%% Covers kura_repo_worker line 654: delete returning not_found for missing record
delete_not_found(_Config) ->
    CS = kura_changeset:cast(kura_test_schema, #{id => 999999999}, #{}, []),
    {error, not_found} = kura_test_repo:delete(CS).

%%======================================================================
%% repo_worker_errors: non-DB unit-like tests
%%======================================================================

%% Covers build_log_event with #{rows := ...} result (line 798)
build_log_event_rows_key(_Config) ->
    Event = kura_repo_worker:build_log_event(
        kura_test_repo, <<"SELECT 1">>, [], #{rows => [#{a => 1}, #{a => 2}]}, 100
    ),
    ?assertEqual(ok, maps:get(result, Event)),
    ?assertEqual(2, maps:get(num_rows, Event)).

%% Covers kura_query_compiler lines 496-497: RIGHT JOIN and FULL JOIN
right_full_join_queries(_Config) ->
    Q1 = kura_query:from(kura_test_schema),
    Q2 = kura_query:join(Q1, right, posts, {id, author_id}),
    Q3 = kura_query:select(Q2, [name]),
    {ok, _} = kura_test_repo:all(Q3),

    Q4 = kura_query:from(kura_test_schema),
    Q5 = kura_query:join(Q4, full, posts, {id, author_id}),
    Q6 = kura_query:select(Q5, [name]),
    {ok, _} = kura_test_repo:all(Q6).

%%======================================================================
%% preloader_edges
%%======================================================================

%% Covers kura_preloader line 64: belongs_to with all nil/undefined FK values
preload_belongs_to_all_nil_fks(_Config) ->
    {ok, User} = insert_user(<<"C2NilFK">>, <<"c2nilfk@test.com">>),
    UserId = maps:get(id, User),
    {ok, Post} = insert_post(<<"C2NilFK Post">>, <<"body">>, UserId),
    PostNoFK = Post#{author_id => undefined},
    [Result] = kura_test_repo:preload(kura_test_post_schema, [PostNoFK], [author]),
    ?assertEqual(nil, maps:get(author, Result)).

%% Covers kura_preloader line 36: nested preload where value is nil -> returns R
preload_nested_nil_stays_nil(_Config) ->
    {ok, User} = insert_user(<<"C2NN">>, <<"c2nn@test.com">>),
    Loaded = kura_test_repo:preload(kura_test_schema, User, [{profile, [user]}]),
    ?assertEqual(nil, maps:get(profile, Loaded)).

%%======================================================================
%% stream_edges
%%======================================================================

%% Covers kura_stream lines via real streaming with batch_size 1
stream_with_batch_size_1(_Config) ->
    {ok, _} = insert_user(<<"C2S_1">>, <<"c2s1@test.com">>),
    {ok, _} = insert_user(<<"C2S_2">>, <<"c2s2@test.com">>),
    Q = kura_query:where(kura_query:from(kura_test_schema), {name, like, <<"C2S_%">>}),
    Self = self(),
    ok = kura_stream:stream(
        kura_test_repo,
        Q,
        fun(Batch) ->
            Self ! {c2_batch, length(Batch)},
            ok
        end,
        #{batch_size => 1}
    ),
    BatchSizes = collect_batches([]),
    ?assert(length(BatchSizes) >= 2),
    ?assertEqual(2, lists:sum(BatchSizes)).

collect_batches(Acc) ->
    receive
        {c2_batch, N} -> collect_batches([N | Acc])
    after 200 ->
        lists:reverse(Acc)
    end.

%%======================================================================
%% migrator_edges
%%======================================================================

%% Covers kura_migrator line 544: quote(atom)
migrator_quote_atom(_Config) ->
    SQL = kura_migrator:compile_operation({drop_table, my_table}),
    ?assertEqual(<<"DROP TABLE \"my_table\"">>, SQL).

%% Covers kura_migrator compile_operation 5-arity create_index (line 264-287)
migrator_compile_index_5_arity(_Config) ->
    SQL = kura_migrator:compile_operation(
        {create_index, <<"my_idx">>, <<"users">>, [email], [unique]}
    ),
    ?assert(binary:match(SQL, <<"UNIQUE">>) =/= nomatch),
    ?assert(binary:match(SQL, <<"\"my_idx\"">>) =/= nomatch).

%% Covers kura_migrator compile_operation 5-arity with where proplist (line 272-274)
migrator_compile_index_where_proplist(_Config) ->
    SQL = kura_migrator:compile_operation(
        {create_index, <<"my_idx2">>, <<"users">>, [email], [{where, <<"active = true">>}]}
    ),
    ?assert(binary:match(SQL, <<"WHERE active = true">>) =/= nomatch).

%%======================================================================
%% query_compiler_edges
%%======================================================================

%% Covers kura_query_compiler line 624: constraint replace_all
on_conflict_constraint_replace_all(_Config) ->
    kura_test_repo:query("ALTER TABLE users ADD CONSTRAINT cov2_email_u5 UNIQUE (email)", []),
    {ok, _} = insert_user(<<"C2CRA">>, <<"c2cra@test.com">>),
    CS = kura_changeset:cast(
        kura_test_schema,
        #{},
        #{name => <<"C2CRA2">>, email => <<"c2cra@test.com">>},
        [name, email]
    ),
    Result = kura_repo_worker:insert(
        kura_test_repo,
        CS,
        #{on_conflict => {{constraint, <<"cov2_email_u5">>}, replace_all}}
    ),
    ?assertMatch({ok, _}, Result),
    kura_test_repo:query("ALTER TABLE users DROP CONSTRAINT IF EXISTS cov2_email_u5", []).

%% Covers kura_query_compiler line 636: constraint {replace, Fields}
on_conflict_constraint_replace(_Config) ->
    kura_test_repo:query("ALTER TABLE users ADD CONSTRAINT cov2_email_u6 UNIQUE (email)", []),
    {ok, _} = insert_user(<<"C2CRF">>, <<"c2crf@test.com">>),
    CS = kura_changeset:cast(
        kura_test_schema,
        #{},
        #{name => <<"C2CRF2">>, email => <<"c2crf@test.com">>},
        [name, email]
    ),
    Result = kura_repo_worker:insert(
        kura_test_repo,
        CS,
        #{on_conflict => {{constraint, <<"cov2_email_u6">>}, {replace, [name]}}}
    ),
    ?assertMatch({ok, _}, Result),
    kura_test_repo:query("ALTER TABLE users DROP CONSTRAINT IF EXISTS cov2_email_u6", []).

%%======================================================================
%% schema_edges
%%======================================================================

%% Covers kura_schema line 105: no primary key defined
schema_no_primary_key(_Config) ->
    catch persistent_term:erase({kura_schema, primary_key, kura_cov2_nopk_schema}),
    ?assertError(
        {no_primary_key, kura_cov2_nopk_schema},
        kura_schema:primary_key(kura_cov2_nopk_schema)
    ).

%% Covers kura_schema line 114: primary_key_field returns undefined
schema_primary_key_field_undefined(_Config) ->
    ?assertEqual(undefined, kura_schema:primary_key_field(kura_cov2_nopk_schema)).

%% Covers kura_schema line 78: field with explicit column name
schema_column_map_explicit_column(_Config) ->
    catch persistent_term:erase({kura_schema, column_map, kura_cov2_colmap_schema}),
    Map = kura_schema:column_map(kura_cov2_colmap_schema),
    ?assertEqual(<<"custom_col">>, maps:get(name, Map)).

%%======================================================================
%% sandbox_edges
%%======================================================================

%% Covers kura_sandbox lines 108, 114: checkin when no connection exists
sandbox_checkin_no_conn(_Config) ->
    kura_sandbox:start(),
    ok = kura_sandbox:checkin(kura_test_repo).

%% Covers kura_sandbox line 164: allowed lookup finds owner but owner has no conn
sandbox_allowed_lookup_miss(_Config) ->
    kura_sandbox:start(),
    Pool = maps:get(pool, kura_repo:config(kura_test_repo)),
    FakePid = spawn(fun() -> ok end),
    timer:sleep(50),
    ets:insert(kura_sandbox_registry, {{Pool, self(), allowed}, FakePid}),
    Result = kura_sandbox:get_conn(Pool),
    ?assertEqual(not_found, Result),
    ets:delete(kura_sandbox_registry, {Pool, self(), allowed}).

%%======================================================================
%% changeset_edges
%%======================================================================

%% Covers kura_changeset line 453: is_blank(null) -> true
is_blank_null(_Config) ->
    CS = kura_changeset:cast(kura_test_schema, #{}, #{name => null, email => <<"x">>}, [name, email]),
    CS1 = kura_changeset:validate_required(CS, [name]),
    ?assertNot(CS1#kura_changeset.valid),
    ?assert(lists:keymember(name, 1, CS1#kura_changeset.errors)).

%% Covers kura_changeset lines 581-589: build_schema_constraints with check constraint
build_schema_constraints_check(_Config) ->
    %% cast/4 with a schema that has constraints/0 returning {check, ...}
    %% triggers build_schema_constraints
    CS = kura_changeset:cast(kura_cov2_constrained_schema, #{}, #{name => <<"x">>}, [name]),
    %% The check constraint should be in the constraints list
    CheckConstraints = [C || C <- CS#kura_changeset.constraints, C#kura_constraint.type =:= check],
    ?assert(length(CheckConstraints) > 0),
    %% And unique constraints from constraints/0
    UniqueConstraints = [
        C
     || C <- CS#kura_changeset.constraints, C#kura_constraint.type =:= unique
    ],
    ?assert(length(UniqueConstraints) > 0).

%% Covers kura_changeset_assoc line 161: existing child without PK in lookup
cast_assoc_existing_child_no_pk(_Config) ->
    CS = kura_changeset:cast(
        kura_test_post_schema,
        #{id => 1, comments => [#{body => <<"old">>, author_id => 1}]},
        #{comments => [#{body => <<"new">>, author_id => 1}]},
        []
    ),
    CS1 = kura_changeset:cast_assoc(CS, comments),
    ?assert(CS1#kura_changeset.valid).

%%======================================================================
%% types_edges
%%======================================================================

%% Covers kura_types format_type({enum, _}) line 377 via catch-all cast error
cast_enum_int_error(_Config) ->
    {error, Msg} = kura_types:cast({enum, [a, b]}, 42),
    ?assert(binary:match(Msg, <<"enum">>) =/= nomatch).

%% Covers kura_types format_type({array, _}) line 379 via catch-all cast error
cast_array_non_list_error(_Config) ->
    {error, Msg} = kura_types:cast({array, string}, not_a_list),
    ?assert(binary:match(Msg, <<"array">>) =/= nomatch).

%% Covers kura_types lines 399, 401, 405, 411: dump_embed nested embeds
dump_embed_one_nested(_Config) ->
    Map = #{
        label => <<"outer">>,
        count => <<"not_an_integer">>,
        address => #{street => <<"Main">>, city => <<"NYC">>},
        labels => [#{label => <<"a">>, weight => 1}, #{label => <<"b">>, weight => 2}]
    },
    {ok, Json} = kura_types:dump({embed, embeds_one, kura_cov2_nested_embed_schema}, Map),
    ?assert(is_binary(Json)),
    Decoded = json:decode(Json),
    ?assert(is_map(Decoded)),
    %% count with bad type -> dump error fallback (line 405) -> value kept as-is
    ?assertEqual(<<"not_an_integer">>, maps:get(<<"count">>, Decoded)),
    %% address nested embed (line 399) -> recursed into dump_embed_to_term
    ?assertEqual(<<"Main">>, maps:get(<<"street">>, maps:get(<<"address">>, Decoded))),
    %% labels nested embed list (line 401)
    Labels = maps:get(<<"labels">>, Decoded),
    ?assertEqual(2, length(Labels)),
    %% zip not in address map -> line 411 (field missing from input)
    Address = maps:get(<<"address">>, Decoded),
    ?assertNot(maps:is_key(<<"zip">>, Address)).

%% Covers kura_types lines 425, 427, 431, 434, 441, 444: load_embed nested
load_embed_one_nested(_Config) ->
    %% Binary keys -> covers normalize_key binary path, and badarg catch (line 441)
    Json = #{
        <<"label">> => <<"outer">>,
        <<"count">> => <<"not_an_integer">>,
        <<"address">> => #{<<"street">> => <<"Main">>, <<"city">> => <<"NYC">>},
        <<"labels">> => [#{<<"label">> => <<"a">>, <<"weight">> => 1}],
        <<"xyzzy_nonexistent_field_9z7">> => <<"extra">>
    },
    {ok, Loaded} = kura_types:load({embed, embeds_one, kura_cov2_nested_embed_schema}, Json),
    ?assert(is_map(Loaded)),
    ?assertEqual(<<"outer">>, maps:get(label, Loaded)),
    %% count had load error (string vs integer) -> value kept as-is (line 431)
    ?assertEqual(<<"not_an_integer">>, maps:get(count, Loaded)),
    Address = maps:get(address, Loaded),
    ?assertEqual(<<"Main">>, maps:get(street, Address)),
    %% Also test with atom keys -> covers normalize_key atom path (line 444)
    Json2 = #{
        label => <<"atom_keyed">>,
        count => 10
    },
    {ok, Loaded2} = kura_types:load({embed, embeds_one, kura_cov2_nested_embed_schema}, Json2),
    ?assertEqual(<<"atom_keyed">>, maps:get(label, Loaded2)).

%% Covers kura_types line 336: format_uuid passthrough (non-standard format)
format_uuid_passthrough(_Config) ->
    %% Line 336 format_uuid passthrough is unreachable through public API
    %% (cast(uuid,...) only calls format_uuid for 32-byte binaries which always match)
    ok.
