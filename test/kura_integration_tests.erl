-module(kura_integration_tests).

-include_lib("eunit/include/eunit.hrl").
-include("kura.hrl").

%%----------------------------------------------------------------------
%% Test fixture
%%----------------------------------------------------------------------

integration_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(_) ->
        [
            %% Basic CRUD
            {"insert returns record with id and timestamps", fun t_insert/0},
            {"get fetches by primary key", fun t_get/0},
            {"get returns not_found for missing id", fun t_get_not_found/0},
            {"get_by fetches by field", fun t_get_by/0},
            {"get_by with multiple clauses", fun t_get_by_multiple_clauses/0},
            {"get_by returns multiple_results error", fun t_get_by_multiple_results/0},
            {"update modifies record", fun t_update/0},
            {"update no-op when no changes", fun t_update_noop/0},
            {"update bumps updated_at timestamp", fun t_update_timestamps/0},
            {"delete removes record", fun t_delete/0},
            {"invalid changeset returns error without hitting DB", fun t_validation/0},

            %% one/1
            {"one returns single result", fun t_one/0},
            {"one returns not_found on no match", fun t_one_not_found/0},

            %% WHERE operators
            {"where equality", fun t_where_eq/0},
            {"where not equal", fun t_where_neq/0},
            {"where less than", fun t_where_lt/0},
            {"where greater than", fun t_where_gt/0},
            {"where less than or equal", fun t_where_lte/0},
            {"where greater than or equal", fun t_where_gte/0},
            {"where like", fun t_where_like/0},
            {"where ilike", fun t_where_ilike/0},
            {"where between", fun t_where_between/0},
            {"where in", fun t_where_in/0},
            {"where not_in", fun t_where_not_in/0},
            {"where is_nil", fun t_where_is_nil/0},
            {"where is_not_nil", fun t_where_is_not_nil/0},
            {"where AND compound", fun t_where_and/0},
            {"where OR compound", fun t_where_or/0},
            {"where NOT", fun t_where_not/0},
            {"where fragment", fun t_where_fragment/0},

            %% Query modifiers
            {"select specific fields", fun t_select_fields/0},
            {"query with order_by/limit", fun t_query/0},
            {"offset skips rows", fun t_offset/0},
            {"distinct removes duplicates", fun t_distinct/0},
            {"group_by and having", fun t_group_by_having/0},
            {"lock for update inside transaction", fun t_lock_for_update/0},

            %% Aggregates
            {"count aggregate", fun t_count/0},
            {"sum aggregate", fun t_sum/0},
            {"avg aggregate", fun t_avg/0},
            {"min/max aggregates", fun t_min_max/0},

            %% Bulk ops
            {"insert_all bulk inserts", fun t_insert_all/0},
            {"update_all bulk updates", fun t_update_all/0},
            {"delete_all bulk deletes", fun t_delete_all/0},

            %% on_conflict
            {"insert on_conflict nothing", fun t_insert_on_conflict_nothing/0},
            {"insert on_conflict replace_all", fun t_insert_on_conflict_replace/0},

            %% Multi
            {"multi success pipeline", fun t_multi_success/0},
            {"multi failure rolls back", fun t_multi_failure/0},
            {"multi with delete step", fun t_multi_delete/0},
            {"multi with run step", fun t_multi_run/0},

            %% Constraints
            {"unique_constraint maps DB error", fun t_unique_constraint/0},
            {"unique_constraint default mapping without declaration",
                fun t_unique_constraint_default/0},
            {"foreign_key_constraint maps DB error", fun t_foreign_key_constraint/0},
            {"check_constraint maps DB error", fun t_check_constraint/0},

            %% Transactions
            {"transaction rollback", fun t_transaction_rollback/0},

            %% Type round-trips
            {"boolean roundtrip", fun t_type_boolean/0},
            {"integer and float roundtrip", fun t_type_int_float/0},
            {"jsonb roundtrip", fun t_type_jsonb/0},
            {"array roundtrip", fun t_type_array/0},

            %% Raw queries
            {"raw SQL query", fun t_raw_query/0},

            %% exists/reload
            {"exists returns true when record matches", fun t_exists_true/0},
            {"exists returns false when no match", fun t_exists_false/0},
            {"reload re-fetches after update", fun t_reload/0},
            {"reload returns not_found for deleted record", fun t_reload_deleted/0},
            {"reload returns error when PK missing", fun t_reload_no_pk/0},

            %% Subqueries
            {"subquery in WHERE", fun t_subquery_in/0},
            {"exists subquery", fun t_exists_subquery/0},

            %% CTEs
            {"CTE with query", fun t_cte/0},

            %% UNION / INTERSECT / EXCEPT
            {"union combines results", fun t_union/0},
            {"intersect finds common", fun t_intersect/0},
            {"except removes matching", fun t_except/0},

            %% Window functions
            {"select_expr with window function", fun t_window_function/0},

            %% insert_all with RETURNING
            {"insert_all returning true", fun t_insert_all_returning_true/0},
            {"insert_all returning fields", fun t_insert_all_returning_fields/0},

            %% prepare_changes
            {"prepare_changes runs before insert", fun t_prepare_changes_insert/0},

            %% Optimistic locking
            {"optimistic lock success", fun t_optimistic_lock_ok/0},
            {"optimistic lock stale", fun t_optimistic_lock_stale/0},

            %% Streaming
            {"stream processes batches", fun t_stream/0},

            %% Scopes
            {"query scopes compose", fun t_scope/0}
        ]
    end}.

setup() ->
    application:ensure_all_started(pgo),
    application:ensure_all_started(kura),
    kura_test_repo:start(),
    {ok, _} = kura_test_repo:query(
        "CREATE TABLE users ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  name VARCHAR(255) NOT NULL,"
        "  email VARCHAR(255) NOT NULL UNIQUE,"
        "  age INTEGER CONSTRAINT users_age_check CHECK (age >= 0),"
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
        "CREATE TABLE posts_simple ("
        "  id BIGSERIAL PRIMARY KEY,"
        "  title VARCHAR(255) NOT NULL,"
        "  author_id BIGINT NOT NULL REFERENCES users(id),"
        "  inserted_at TIMESTAMPTZ,"
        "  updated_at TIMESTAMPTZ"
        ")",
        []
    ),
    ok.

teardown(_) ->
    kura_test_repo:query("DROP TABLE IF EXISTS posts_simple CASCADE", []),
    kura_test_repo:query("DROP TABLE IF EXISTS users CASCADE", []),
    ok.

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

insert_user(Name, Email) ->
    CS = kura_changeset:cast(
        kura_test_schema,
        #{},
        #{<<"name">> => Name, <<"email">> => Email},
        [name, email]
    ),
    CS1 = kura_changeset:validate_required(CS, [name, email]),
    kura_test_repo:insert(CS1).

insert_user(Name, Email, Fields) ->
    CS = kura_changeset:cast(
        kura_test_schema,
        #{},
        Fields#{<<"name">> => Name, <<"email">> => Email},
        [name, email, age, active, role, score, metadata, tags]
    ),
    CS1 = kura_changeset:validate_required(CS, [name, email]),
    kura_test_repo:insert(CS1).

%%----------------------------------------------------------------------
%% Basic CRUD
%%----------------------------------------------------------------------

t_insert() ->
    {ok, User} = insert_user(<<"Alice">>, <<"alice@example.com">>),
    ?assert(is_integer(maps:get(id, User))),
    ?assertEqual(<<"Alice">>, maps:get(name, User)),
    ?assertEqual(<<"alice@example.com">>, maps:get(email, User)),
    ?assertNotEqual(undefined, maps:get(inserted_at, User)),
    ?assertNotEqual(undefined, maps:get(updated_at, User)).

t_get() ->
    {ok, Inserted} = insert_user(<<"Bob">>, <<"bob@example.com">>),
    Id = maps:get(id, Inserted),
    {ok, Fetched} = kura_test_repo:get(kura_test_schema, Id),
    ?assertEqual(<<"Bob">>, maps:get(name, Fetched)),
    ?assertEqual(<<"bob@example.com">>, maps:get(email, Fetched)),
    ?assertEqual(Id, maps:get(id, Fetched)).

t_get_not_found() ->
    ?assertEqual({error, not_found}, kura_test_repo:get(kura_test_schema, 999999)).

t_get_by() ->
    {ok, _} = insert_user(<<"Carol">>, <<"carol@example.com">>),
    {ok, Found} = kura_test_repo:get_by(kura_test_schema, [{email, <<"carol@example.com">>}]),
    ?assertEqual(<<"Carol">>, maps:get(name, Found)).

t_get_by_multiple_clauses() ->
    {ok, _} = insert_user(
        <<"MultiClause">>,
        <<"multiclause@example.com">>,
        #{<<"age">> => 30}
    ),
    {ok, Found} = kura_test_repo:get_by(
        kura_test_schema,
        [{email, <<"multiclause@example.com">>}, {name, <<"MultiClause">>}]
    ),
    ?assertEqual(30, maps:get(age, Found)).

t_get_by_multiple_results() ->
    %% Insert two users with same name (different emails to satisfy UNIQUE)
    {ok, _} = insert_user(<<"DupName">>, <<"dupname1@example.com">>),
    {ok, _} = insert_user(<<"DupName">>, <<"dupname2@example.com">>),
    ?assertEqual(
        {error, multiple_results},
        kura_test_repo:get_by(kura_test_schema, [{name, <<"DupName">>}])
    ).

t_update() ->
    {ok, User} = insert_user(<<"Grace">>, <<"grace@example.com">>),
    CS = kura_changeset:cast(
        kura_test_schema,
        User,
        #{<<"name">> => <<"Grace Updated">>},
        [name]
    ),
    {ok, Updated} = kura_test_repo:update(CS),
    ?assertEqual(<<"Grace Updated">>, maps:get(name, Updated)),
    ?assertEqual(maps:get(id, User), maps:get(id, Updated)),
    {ok, Fetched} = kura_test_repo:get(kura_test_schema, maps:get(id, User)),
    ?assertEqual(<<"Grace Updated">>, maps:get(name, Fetched)).

t_update_noop() ->
    {ok, User} = insert_user(<<"NoopUser">>, <<"noop@example.com">>),
    %% Cast with same value — no changes
    CS = kura_changeset:cast(kura_test_schema, User, #{<<"name">> => <<"NoopUser">>}, [name]),
    {ok, Result} = kura_test_repo:update(CS),
    ?assertEqual(maps:get(id, User), maps:get(id, Result)).

t_update_timestamps() ->
    {ok, User} = insert_user(<<"TSUser">>, <<"tsuser@example.com">>),
    OrigUpdatedAt = maps:get(updated_at, User),

    %% Sleep >1s to ensure timestamp differs (PG timestamp has second resolution)
    timer:sleep(1100),
    CS = kura_changeset:cast(kura_test_schema, User, #{<<"name">> => <<"TSUser2">>}, [name]),
    {ok, Updated} = kura_test_repo:update(CS),
    NewUpdatedAt = maps:get(updated_at, Updated),
    ?assertNotEqual(OrigUpdatedAt, NewUpdatedAt).

t_delete() ->
    {ok, User} = insert_user(<<"Hank">>, <<"hank@example.com">>),
    Id = maps:get(id, User),
    CS = kura_changeset:cast(kura_test_schema, User, #{}, []),
    {ok, _Deleted} = kura_test_repo:delete(CS),
    ?assertEqual({error, not_found}, kura_test_repo:get(kura_test_schema, Id)).

t_validation() ->
    CS = kura_changeset:cast(
        kura_test_schema, #{}, #{<<"name">> => <<>>}, [name, email]
    ),
    CS1 = kura_changeset:validate_required(CS, [name, email]),
    CS2 = kura_changeset:validate_length(CS1, name, [{min, 1}]),
    ?assertNot(CS2#kura_changeset.valid),
    {error, InvalidCS} = kura_test_repo:insert(CS2),
    ?assertNot(InvalidCS#kura_changeset.valid),
    ?assertEqual(insert, InvalidCS#kura_changeset.action).

%%----------------------------------------------------------------------
%% one/1
%%----------------------------------------------------------------------

t_one() ->
    {ok, _} = insert_user(<<"OneUser">>, <<"oneuser@example.com">>),
    Q = kura_query:where(kura_query:from(kura_test_schema), {name, <<"OneUser">>}),
    {ok, Found} = kura_test_repo:one(Q),
    ?assertEqual(<<"OneUser">>, maps:get(name, Found)).

t_one_not_found() ->
    Q = kura_query:where(kura_query:from(kura_test_schema), {name, <<"NoSuchUserEver_12345">>}),
    ?assertEqual({error, not_found}, kura_test_repo:one(Q)).

%%----------------------------------------------------------------------
%% WHERE operators
%%----------------------------------------------------------------------

t_where_eq() ->
    {ok, _} = insert_user(<<"EqUser">>, <<"eq@example.com">>, #{<<"age">> => 25}),
    Q = kura_query:where(kura_query:from(kura_test_schema), {email, '=', <<"eq@example.com">>}),
    {ok, [Row]} = kura_test_repo:all(Q),
    ?assertEqual(<<"EqUser">>, maps:get(name, Row)).

t_where_neq() ->
    {ok, _} = insert_user(<<"NeqA">>, <<"neqa@example.com">>, #{<<"age">> => 10}),
    {ok, _} = insert_user(<<"NeqB">>, <<"neqb@example.com">>, #{<<"age">> => 20}),
    Q = kura_query:where(
        kura_query:from(kura_test_schema),
        {'and', [{age, '!=', 10}, {name, in, [<<"NeqA">>, <<"NeqB">>]}]}
    ),
    {ok, Results} = kura_test_repo:all(Q),
    ?assertEqual(1, length(Results)),
    ?assertEqual(<<"NeqB">>, maps:get(name, hd(Results))).

t_where_lt() ->
    {ok, _} = insert_user(<<"LtA">>, <<"lta@example.com">>, #{<<"age">> => 18}),
    {ok, _} = insert_user(<<"LtB">>, <<"ltb@example.com">>, #{<<"age">> => 30}),
    Q = kura_query:where(
        kura_query:from(kura_test_schema),
        {'and', [{age, '<', 20}, {name, in, [<<"LtA">>, <<"LtB">>]}]}
    ),
    {ok, Results} = kura_test_repo:all(Q),
    ?assertEqual(1, length(Results)),
    ?assertEqual(<<"LtA">>, maps:get(name, hd(Results))).

t_where_gt() ->
    {ok, _} = insert_user(<<"GtA">>, <<"gta@example.com">>, #{<<"age">> => 18}),
    {ok, _} = insert_user(<<"GtB">>, <<"gtb@example.com">>, #{<<"age">> => 30}),
    Q = kura_query:where(
        kura_query:from(kura_test_schema),
        {'and', [{age, '>', 20}, {name, in, [<<"GtA">>, <<"GtB">>]}]}
    ),
    {ok, Results} = kura_test_repo:all(Q),
    ?assertEqual(1, length(Results)),
    ?assertEqual(<<"GtB">>, maps:get(name, hd(Results))).

t_where_lte() ->
    {ok, _} = insert_user(<<"LteA">>, <<"ltea@example.com">>, #{<<"age">> => 20}),
    {ok, _} = insert_user(<<"LteB">>, <<"lteb@example.com">>, #{<<"age">> => 30}),
    Q = kura_query:where(
        kura_query:from(kura_test_schema),
        {'and', [{age, '<=', 20}, {name, in, [<<"LteA">>, <<"LteB">>]}]}
    ),
    {ok, Results} = kura_test_repo:all(Q),
    ?assertEqual(1, length(Results)),
    ?assertEqual(<<"LteA">>, maps:get(name, hd(Results))).

t_where_gte() ->
    {ok, _} = insert_user(<<"GteA">>, <<"gtea@example.com">>, #{<<"age">> => 20}),
    {ok, _} = insert_user(<<"GteB">>, <<"gteb@example.com">>, #{<<"age">> => 30}),
    Q = kura_query:where(
        kura_query:from(kura_test_schema),
        {'and', [{age, '>=', 30}, {name, in, [<<"GteA">>, <<"GteB">>]}]}
    ),
    {ok, Results} = kura_test_repo:all(Q),
    ?assertEqual(1, length(Results)),
    ?assertEqual(<<"GteB">>, maps:get(name, hd(Results))).

t_where_like() ->
    {ok, _} = insert_user(<<"LikeUser">>, <<"likeuser@example.com">>),
    Q = kura_query:where(kura_query:from(kura_test_schema), {name, like, <<"Like%">>}),
    {ok, Results} = kura_test_repo:all(Q),
    ?assert(length(Results) >= 1),
    ?assert(lists:any(fun(R) -> maps:get(name, R) =:= <<"LikeUser">> end, Results)).

t_where_ilike() ->
    {ok, _} = insert_user(<<"IlikeUser">>, <<"ilikeuser@example.com">>),
    Q = kura_query:where(kura_query:from(kura_test_schema), {name, ilike, <<"ilike%">>}),
    {ok, Results} = kura_test_repo:all(Q),
    ?assert(length(Results) >= 1),
    ?assert(lists:any(fun(R) -> maps:get(name, R) =:= <<"IlikeUser">> end, Results)).

t_where_between() ->
    {ok, _} = insert_user(<<"BtwA">>, <<"btwa@example.com">>, #{<<"age">> => 15}),
    {ok, _} = insert_user(<<"BtwB">>, <<"btwb@example.com">>, #{<<"age">> => 25}),
    {ok, _} = insert_user(<<"BtwC">>, <<"btwc@example.com">>, #{<<"age">> => 35}),
    Q = kura_query:where(
        kura_query:from(kura_test_schema),
        {'and', [{age, between, {20, 30}}, {name, in, [<<"BtwA">>, <<"BtwB">>, <<"BtwC">>]}]}
    ),
    {ok, Results} = kura_test_repo:all(Q),
    ?assertEqual(1, length(Results)),
    ?assertEqual(<<"BtwB">>, maps:get(name, hd(Results))).

t_where_in() ->
    {ok, _} = insert_user(<<"InA">>, <<"ina@example.com">>),
    {ok, _} = insert_user(<<"InB">>, <<"inb@example.com">>),
    {ok, _} = insert_user(<<"InC">>, <<"inc@example.com">>),
    Q = kura_query:where(
        kura_query:from(kura_test_schema),
        {name, in, [<<"InA">>, <<"InC">>]}
    ),
    Q1 = kura_query:order_by(Q, [{name, asc}]),
    {ok, Results} = kura_test_repo:all(Q1),
    Names = [maps:get(name, R) || R <- Results],
    ?assert(lists:member(<<"InA">>, Names)),
    ?assert(lists:member(<<"InC">>, Names)),
    ?assertNot(lists:member(<<"InB">>, Names)).

t_where_not_in() ->
    {ok, _} = insert_user(<<"NinA">>, <<"nina@example.com">>),
    {ok, _} = insert_user(<<"NinB">>, <<"ninb@example.com">>),
    Q = kura_query:where(
        kura_query:from(kura_test_schema),
        {'and', [{name, not_in, [<<"NinA">>]}, {name, in, [<<"NinA">>, <<"NinB">>]}]}
    ),
    {ok, Results} = kura_test_repo:all(Q),
    ?assertEqual(1, length(Results)),
    ?assertEqual(<<"NinB">>, maps:get(name, hd(Results))).

t_where_is_nil() ->
    {ok, _} = insert_user(<<"NilAge">>, <<"nilage@example.com">>),
    {ok, _} = insert_user(<<"HasAge">>, <<"hasage@example.com">>, #{<<"age">> => 25}),
    Q = kura_query:where(
        kura_query:from(kura_test_schema),
        {'and', [{age, is_nil}, {name, in, [<<"NilAge">>, <<"HasAge">>]}]}
    ),
    {ok, Results} = kura_test_repo:all(Q),
    ?assertEqual(1, length(Results)),
    ?assertEqual(<<"NilAge">>, maps:get(name, hd(Results))).

t_where_is_not_nil() ->
    {ok, _} = insert_user(<<"NotNilA">>, <<"notnila@example.com">>),
    {ok, _} = insert_user(<<"NotNilB">>, <<"notnilb@example.com">>, #{<<"age">> => 42}),
    Q = kura_query:where(
        kura_query:from(kura_test_schema),
        {'and', [{age, is_not_nil}, {name, in, [<<"NotNilA">>, <<"NotNilB">>]}]}
    ),
    {ok, Results} = kura_test_repo:all(Q),
    ?assertEqual(1, length(Results)),
    ?assertEqual(<<"NotNilB">>, maps:get(name, hd(Results))).

t_where_and() ->
    {ok, _} = insert_user(<<"AndUser">>, <<"anduser@example.com">>, #{<<"age">> => 25}),
    Q = kura_query:where(
        kura_query:from(kura_test_schema),
        {'and', [{name, <<"AndUser">>}, {age, 25}]}
    ),
    {ok, Results} = kura_test_repo:all(Q),
    ?assertEqual(1, length(Results)).

t_where_or() ->
    {ok, _} = insert_user(<<"OrA">>, <<"ora@example.com">>, #{<<"age">> => 10}),
    {ok, _} = insert_user(<<"OrB">>, <<"orb@example.com">>, #{<<"age">> => 50}),
    Q = kura_query:where(
        kura_query:from(kura_test_schema),
        {'and', [
            {'or', [{age, 10}, {age, 50}]},
            {name, in, [<<"OrA">>, <<"OrB">>]}
        ]}
    ),
    Q1 = kura_query:order_by(Q, [{name, asc}]),
    {ok, Results} = kura_test_repo:all(Q1),
    ?assertEqual(2, length(Results)),
    ?assertEqual(<<"OrA">>, maps:get(name, hd(Results))).

t_where_not() ->
    {ok, _} = insert_user(<<"NotA">>, <<"nota@example.com">>, #{<<"age">> => 10}),
    {ok, _} = insert_user(<<"NotB">>, <<"notb@example.com">>, #{<<"age">> => 20}),
    Q = kura_query:where(
        kura_query:from(kura_test_schema),
        {'and', [
            {'not', {age, 10}},
            {name, in, [<<"NotA">>, <<"NotB">>]}
        ]}
    ),
    {ok, Results} = kura_test_repo:all(Q),
    ?assertEqual(1, length(Results)),
    ?assertEqual(<<"NotB">>, maps:get(name, hd(Results))).

t_where_fragment() ->
    {ok, _} = insert_user(<<"FragUser">>, <<"fraguser@example.com">>, #{<<"age">> => 33}),
    Q = kura_query:where(
        kura_query:from(kura_test_schema),
        {fragment, <<"\"age\" = ? AND \"name\" = ?">>, [33, <<"FragUser">>]}
    ),
    {ok, Results} = kura_test_repo:all(Q),
    ?assert(length(Results) >= 1),
    ?assert(lists:any(fun(R) -> maps:get(name, R) =:= <<"FragUser">> end, Results)).

%%----------------------------------------------------------------------
%% Query modifiers
%%----------------------------------------------------------------------

t_select_fields() ->
    {ok, _} = insert_user(<<"SelUser">>, <<"seluser@example.com">>),
    Q = kura_query:from(kura_test_schema),
    Q1 = kura_query:where(Q, {email, <<"seluser@example.com">>}),
    Q2 = kura_query:select(Q1, [name, email]),
    {ok, [Row]} = kura_test_repo:all(Q2),
    ?assertEqual(<<"SelUser">>, maps:get(name, Row)),
    ?assertEqual(<<"seluser@example.com">>, maps:get(email, Row)),
    %% Only selected fields should be in the result
    ?assertNot(maps:is_key(age, Row)).

t_query() ->
    {ok, _} = insert_user(<<"Dave">>, <<"dave@example.com">>),
    {ok, _} = insert_user(<<"Eve">>, <<"eve@example.com">>),
    {ok, _} = insert_user(<<"Frank">>, <<"frank@example.com">>),
    Q = kura_query:from(kura_test_schema),
    Q1 = kura_query:where(Q, {name, in, [<<"Dave">>, <<"Eve">>, <<"Frank">>]}),
    Q2 = kura_query:order_by(Q1, [{name, asc}]),
    Q3 = kura_query:limit(Q2, 2),
    {ok, Results} = kura_test_repo:all(Q3),
    ?assertEqual(2, length(Results)),
    ?assertEqual(<<"Dave">>, maps:get(name, hd(Results))).

t_offset() ->
    {ok, _} = insert_user(<<"OffA">>, <<"offa@example.com">>),
    {ok, _} = insert_user(<<"OffB">>, <<"offb@example.com">>),
    {ok, _} = insert_user(<<"OffC">>, <<"offc@example.com">>),
    Q = kura_query:from(kura_test_schema),
    Q1 = kura_query:where(Q, {name, in, [<<"OffA">>, <<"OffB">>, <<"OffC">>]}),
    Q2 = kura_query:order_by(Q1, [{name, asc}]),
    Q3 = kura_query:offset(kura_query:limit(Q2, 2), 1),
    {ok, Results} = kura_test_repo:all(Q3),
    ?assertEqual(2, length(Results)),
    ?assertEqual(<<"OffB">>, maps:get(name, hd(Results))).

t_distinct() ->
    {ok, _} = insert_user(<<"DistA">>, <<"dista@example.com">>, #{<<"role">> => <<"editor">>}),
    {ok, _} = insert_user(<<"DistB">>, <<"distb@example.com">>, #{<<"role">> => <<"editor">>}),
    Q = kura_query:from(kura_test_schema),
    Q1 = kura_query:where(Q, {name, in, [<<"DistA">>, <<"DistB">>]}),
    Q2 = kura_query:select(Q1, [role]),
    Q3 = kura_query:distinct(Q2),
    {ok, Results} = kura_test_repo:all(Q3),
    ?assertEqual(1, length(Results)),
    ?assertEqual(<<"editor">>, maps:get(role, hd(Results))).

t_group_by_having() ->
    {ok, _} = insert_user(<<"GrpA">>, <<"grpa@example.com">>, #{<<"role">> => <<"admin">>}),
    {ok, _} = insert_user(<<"GrpB">>, <<"grpb@example.com">>, #{<<"role">> => <<"admin">>}),
    {ok, _} = insert_user(<<"GrpC">>, <<"grpc@example.com">>, #{<<"role">> => <<"viewer">>}),
    Q = kura_query:from(kura_test_schema),
    Q1 = kura_query:where(Q, {name, in, [<<"GrpA">>, <<"GrpB">>, <<"GrpC">>]}),
    Q2 = kura_query:select(Q1, [role, {count, '*'}]),
    Q3 = kura_query:group_by(Q2, [role]),
    Q4 = kura_query:having(Q3, {fragment, <<"count(*) > ?">>, [1]}),
    {ok, Results} = kura_test_repo:all(Q4),
    ?assertEqual(1, length(Results)),
    ?assertEqual(<<"admin">>, maps:get(role, hd(Results))),
    ?assertEqual(2, maps:get(count, hd(Results))).

t_lock_for_update() ->
    {ok, User} = insert_user(<<"LockUser">>, <<"lockuser@example.com">>),
    Id = maps:get(id, User),
    kura_test_repo:transaction(fun() ->
        Q = kura_query:from(kura_test_schema),
        Q1 = kura_query:where(Q, {id, Id}),
        Q2 = kura_query:lock(Q1, <<"FOR UPDATE">>),
        {ok, [Locked]} = kura_test_repo:all(Q2),
        ?assertEqual(<<"LockUser">>, maps:get(name, Locked))
    end).

%%----------------------------------------------------------------------
%% Aggregates
%%----------------------------------------------------------------------

t_count() ->
    Q = kura_query:from(kura_test_schema),
    Q1 = kura_query:count(Q),
    {ok, [Row]} = kura_test_repo:all(Q1),
    ?assert(maps:get(count, Row) >= 1).

t_sum() ->
    {ok, _} = insert_user(<<"SumA">>, <<"suma@example.com">>, #{<<"score">> => 10.0}),
    {ok, _} = insert_user(<<"SumB">>, <<"sumb@example.com">>, #{<<"score">> => 20.0}),
    Q = kura_query:from(kura_test_schema),
    Q1 = kura_query:where(Q, {name, in, [<<"SumA">>, <<"SumB">>]}),
    Q2 = kura_query:sum(Q1, score),
    {ok, [Row]} = kura_test_repo:all(Q2),
    ?assertEqual(30.0, maps:get(sum, Row)).

t_avg() ->
    {ok, _} = insert_user(<<"AvgA">>, <<"avga@example.com">>, #{<<"score">> => 10.0}),
    {ok, _} = insert_user(<<"AvgB">>, <<"avgb@example.com">>, #{<<"score">> => 30.0}),
    Q = kura_query:from(kura_test_schema),
    Q1 = kura_query:where(Q, {name, in, [<<"AvgA">>, <<"AvgB">>]}),
    Q2 = kura_query:avg(Q1, score),
    {ok, [Row]} = kura_test_repo:all(Q2),
    ?assertEqual(20.0, maps:get(avg, Row)).

t_min_max() ->
    {ok, _} = insert_user(<<"MmA">>, <<"mma@example.com">>, #{<<"age">> => 5}),
    {ok, _} = insert_user(<<"MmB">>, <<"mmb@example.com">>, #{<<"age">> => 99}),
    Q = kura_query:from(kura_test_schema),
    Q1 = kura_query:where(Q, {name, in, [<<"MmA">>, <<"MmB">>]}),

    Q2 = kura_query:min(Q1, age),
    {ok, [MinRow]} = kura_test_repo:all(Q2),
    ?assertEqual(5, maps:get(min, MinRow)),

    Q3 = kura_query:max(Q1, age),
    {ok, [MaxRow]} = kura_test_repo:all(Q3),
    ?assertEqual(99, maps:get(max, MaxRow)).

%%----------------------------------------------------------------------
%% Bulk ops
%%----------------------------------------------------------------------

t_insert_all() ->
    Entries = [
        #{name => <<"Bulk1">>, email => <<"bulk1@example.com">>},
        #{name => <<"Bulk2">>, email => <<"bulk2@example.com">>},
        #{name => <<"Bulk3">>, email => <<"bulk3@example.com">>}
    ],
    {ok, Count} = kura_test_repo:insert_all(kura_test_schema, Entries),
    ?assertEqual(3, Count),
    {ok, Found} = kura_test_repo:get_by(kura_test_schema, [{email, <<"bulk2@example.com">>}]),
    ?assertEqual(<<"Bulk2">>, maps:get(name, Found)).

t_update_all() ->
    {ok, _} = insert_user(<<"UpdAll1">>, <<"updall1@example.com">>),
    {ok, _} = insert_user(<<"UpdAll2">>, <<"updall2@example.com">>),
    Q = kura_query:from(kura_test_schema),
    Q1 = kura_query:where(Q, {name, in, [<<"UpdAll1">>, <<"UpdAll2">>]}),
    {ok, Count} = kura_test_repo:update_all(Q1, #{role => <<"admin">>}),
    ?assertEqual(2, Count),
    {ok, U1} = kura_test_repo:get_by(kura_test_schema, [{email, <<"updall1@example.com">>}]),
    ?assertEqual(<<"admin">>, maps:get(role, U1)).

t_delete_all() ->
    {ok, _} = insert_user(<<"DelAll1">>, <<"delall1@example.com">>),
    {ok, _} = insert_user(<<"DelAll2">>, <<"delall2@example.com">>),
    Q = kura_query:from(kura_test_schema),
    Q1 = kura_query:where(Q, {name, in, [<<"DelAll1">>, <<"DelAll2">>]}),
    {ok, Count} = kura_test_repo:delete_all(Q1),
    ?assertEqual(2, Count),
    ?assertEqual(
        {error, not_found},
        kura_test_repo:get_by(kura_test_schema, [{email, <<"delall1@example.com">>}])
    ).

%%----------------------------------------------------------------------
%% on_conflict
%%----------------------------------------------------------------------

t_insert_on_conflict_nothing() ->
    {ok, First} = insert_user(<<"OcNothing">>, <<"ocnothing@example.com">>),
    %% Try to insert same email — should do nothing
    CS = kura_changeset:cast(
        kura_test_schema,
        #{},
        #{<<"name">> => <<"OcNothing2">>, <<"email">> => <<"ocnothing@example.com">>},
        [name, email]
    ),
    {ok, _Result} = kura_test_repo:insert(CS, #{on_conflict => {email, nothing}}),
    %% Returns applied changeset (no DB row), original stays
    {ok, Fetched} = kura_test_repo:get(kura_test_schema, maps:get(id, First)),
    ?assertEqual(<<"OcNothing">>, maps:get(name, Fetched)).

t_insert_on_conflict_replace() ->
    {ok, _} = insert_user(<<"OcReplace">>, <<"ocreplace@example.com">>),
    %% Insert same email — should update name
    CS = kura_changeset:cast(
        kura_test_schema,
        #{},
        #{<<"name">> => <<"OcReplaced">>, <<"email">> => <<"ocreplace@example.com">>},
        [name, email]
    ),
    {ok, Result} = kura_test_repo:insert(CS, #{on_conflict => {email, replace_all}}),
    ?assertEqual(<<"OcReplaced">>, maps:get(name, Result)),
    ?assertEqual(<<"ocreplace@example.com">>, maps:get(email, Result)).

%%----------------------------------------------------------------------
%% Multi
%%----------------------------------------------------------------------

t_multi_success() ->
    CS1 = kura_changeset:cast(
        kura_test_schema,
        #{},
        #{<<"name">> => <<"Multi1">>, <<"email">> => <<"multi1@example.com">>},
        [name, email]
    ),
    CS1V = kura_changeset:validate_required(CS1, [name, email]),
    M = kura_multi:new(),
    M1 = kura_multi:insert(M, create_user, CS1V),
    M2 = kura_multi:update(M1, update_user, fun(#{create_user := User}) ->
        kura_changeset:cast(kura_test_schema, User, #{<<"name">> => <<"Multi1_Updated">>}, [name])
    end),
    {ok, Results} = kura_test_repo:multi(M2),
    ?assert(is_map(Results)),
    ?assertMatch(#{name := <<"Multi1_Updated">>}, maps:get(update_user, Results)).

t_multi_failure() ->
    CS1 = kura_changeset:cast(
        kura_test_schema,
        #{},
        #{<<"name">> => <<"MultiF1">>, <<"email">> => <<"multif1@example.com">>},
        [name, email]
    ),
    CS1V = kura_changeset:validate_required(CS1, [name, email]),
    M = kura_multi:new(),
    M1 = kura_multi:insert(M, create_user, CS1V),
    M2 = kura_multi:run(M1, fail_step, fun(_) -> {error, <<"intentional fail">>} end),
    Result = kura_test_repo:multi(M2),
    ?assertMatch({error, fail_step, <<"intentional fail">>, _}, Result),
    ?assertEqual(
        {error, not_found},
        kura_test_repo:get_by(kura_test_schema, [{email, <<"multif1@example.com">>}])
    ).

t_multi_delete() ->
    {ok, User} = insert_user(<<"MultiDel">>, <<"multidel@example.com">>),
    M = kura_multi:new(),
    M1 = kura_multi:delete(M, delete_user, fun(_) ->
        kura_changeset:cast(kura_test_schema, User, #{}, [])
    end),
    {ok, Results} = kura_test_repo:multi(M1),
    ?assert(is_map_key(delete_user, Results)),
    ?assertEqual(
        {error, not_found},
        kura_test_repo:get_by(kura_test_schema, [{email, <<"multidel@example.com">>}])
    ).

t_multi_run() ->
    M = kura_multi:new(),
    M1 = kura_multi:run(M, compute, fun(_) -> {ok, 42} end),
    M2 = kura_multi:run(M1, double, fun(#{compute := V}) -> {ok, V * 2} end),
    {ok, Results} = kura_test_repo:multi(M2),
    ?assertEqual(42, maps:get(compute, Results)),
    ?assertEqual(84, maps:get(double, Results)).

%%----------------------------------------------------------------------
%% Constraints
%%----------------------------------------------------------------------

t_unique_constraint() ->
    {ok, _} = insert_user(<<"UniqueUser">>, <<"unique@example.com">>),
    CS = kura_changeset:cast(
        kura_test_schema,
        #{},
        #{<<"name">> => <<"UniqueUser2">>, <<"email">> => <<"unique@example.com">>},
        [name, email]
    ),
    CS1 = kura_changeset:validate_required(CS, [name, email]),
    CS2 = kura_changeset:unique_constraint(CS1, email),
    {error, ErrCS} = kura_test_repo:insert(CS2),
    ?assertNot(ErrCS#kura_changeset.valid),
    ?assert(lists:keymember(email, 1, ErrCS#kura_changeset.errors)),
    %% Check the specific error message from the constraint declaration
    {email, Msg} = lists:keyfind(email, 1, ErrCS#kura_changeset.errors),
    ?assertEqual(<<"has already been taken">>, Msg).

t_unique_constraint_default() ->
    %% Without declaring unique_constraint, the error should still map via default
    {ok, _} = insert_user(<<"DefUQ">>, <<"defuq@example.com">>),
    CS = kura_changeset:cast(
        kura_test_schema,
        #{},
        #{<<"name">> => <<"DefUQ2">>, <<"email">> => <<"defuq@example.com">>},
        [name, email]
    ),
    CS1 = kura_changeset:validate_required(CS, [name, email]),
    {error, ErrCS} = kura_test_repo:insert(CS1),
    ?assertNot(ErrCS#kura_changeset.valid),
    %% Without explicit constraint, maps to field derived from constraint name
    ?assert(lists:keymember(email, 1, ErrCS#kura_changeset.errors)).

t_foreign_key_constraint() ->
    %% Insert into posts_simple with a non-existent author_id
    CS = kura_changeset:cast(
        kura_test_post_simple_schema,
        #{},
        #{<<"title">> => <<"Bad Post">>, <<"author_id">> => 999999},
        [title, author_id]
    ),
    CS1 = kura_changeset:foreign_key_constraint(CS, author_id),
    {error, ErrCS} = kura_test_repo:insert(CS1),
    ?assertNot(ErrCS#kura_changeset.valid),
    ?assert(lists:keymember(author_id, 1, ErrCS#kura_changeset.errors)),
    {author_id, Msg} = lists:keyfind(author_id, 1, ErrCS#kura_changeset.errors),
    ?assertEqual(<<"does not exist">>, Msg).

t_check_constraint() ->
    CS = kura_changeset:cast(
        kura_test_schema,
        #{},
        #{
            <<"name">> => <<"CheckUser">>,
            <<"email">> => <<"checkuser@example.com">>,
            <<"age">> => -5
        },
        [name, email, age]
    ),
    CS1 = kura_changeset:validate_required(CS, [name, email]),
    CS2 = kura_changeset:check_constraint(
        CS1,
        <<"users_age_check">>,
        age,
        #{message => <<"must be non-negative">>}
    ),
    {error, ErrCS} = kura_test_repo:insert(CS2),
    ?assertNot(ErrCS#kura_changeset.valid),
    ?assert(lists:keymember(age, 1, ErrCS#kura_changeset.errors)),
    {age, Msg} = lists:keyfind(age, 1, ErrCS#kura_changeset.errors),
    ?assertEqual(<<"must be non-negative">>, Msg).

%%----------------------------------------------------------------------
%% Transactions
%%----------------------------------------------------------------------

t_transaction_rollback() ->
    ?assertError(
        rollback_test,
        kura_test_repo:transaction(fun() ->
            {ok, _} = insert_user(<<"TxnUser">>, <<"txn@example.com">>),
            error(rollback_test)
        end)
    ),
    ?assertEqual(
        {error, not_found},
        kura_test_repo:get_by(kura_test_schema, [{email, <<"txn@example.com">>}])
    ).

%%----------------------------------------------------------------------
%% Type round-trips
%%----------------------------------------------------------------------

t_type_boolean() ->
    {ok, User} = insert_user(
        <<"BoolUser">>,
        <<"booluser@example.com">>,
        #{<<"active">> => false}
    ),
    ?assertEqual(false, maps:get(active, User)),
    {ok, Fetched} = kura_test_repo:get(kura_test_schema, maps:get(id, User)),
    ?assertEqual(false, maps:get(active, Fetched)).

t_type_int_float() ->
    {ok, User} = insert_user(
        <<"NumUser">>,
        <<"numuser@example.com">>,
        #{<<"age">> => 42, <<"score">> => 3.14}
    ),
    ?assertEqual(42, maps:get(age, User)),
    ?assertEqual(3.14, maps:get(score, User)),
    {ok, Fetched} = kura_test_repo:get(kura_test_schema, maps:get(id, User)),
    ?assertEqual(42, maps:get(age, Fetched)),
    ?assertEqual(3.14, maps:get(score, Fetched)).

t_type_jsonb() ->
    Meta = #{<<"key">> => <<"value">>, <<"nested">> => #{<<"a">> => 1}},
    {ok, User} = insert_user(
        <<"JsonUser">>,
        <<"jsonuser@example.com">>,
        #{<<"metadata">> => Meta}
    ),
    {ok, Fetched} = kura_test_repo:get(kura_test_schema, maps:get(id, User)),
    FetchedMeta = maps:get(metadata, Fetched),
    ?assertEqual(<<"value">>, maps:get(<<"key">>, FetchedMeta)),
    ?assertEqual(#{<<"a">> => 1}, maps:get(<<"nested">>, FetchedMeta)).

t_type_array() ->
    Tags = [<<"erlang">>, <<"otp">>, <<"beam">>],
    {ok, User} = insert_user(
        <<"ArrayUser">>,
        <<"arrayuser@example.com">>,
        #{<<"tags">> => Tags}
    ),
    {ok, Fetched} = kura_test_repo:get(kura_test_schema, maps:get(id, User)),
    ?assertEqual(Tags, maps:get(tags, Fetched)).

%%----------------------------------------------------------------------
%% Raw queries
%%----------------------------------------------------------------------

t_raw_query() ->
    {ok, _} = insert_user(<<"RawUser">>, <<"rawuser@example.com">>),
    {ok, Rows} = kura_test_repo:query(
        "SELECT \"name\", \"email\" FROM \"users\" WHERE \"email\" = $1",
        [<<"rawuser@example.com">>]
    ),
    ?assertEqual(1, length(Rows)),
    Row = hd(Rows),
    ?assertEqual(<<"RawUser">>, maps:get(name, Row)).

%%----------------------------------------------------------------------
%% exists / reload
%%----------------------------------------------------------------------

t_exists_true() ->
    {ok, _} = insert_user(<<"ExistsUser">>, <<"existsuser@example.com">>),
    Q = kura_query:where(kura_query:from(kura_test_schema), {email, <<"existsuser@example.com">>}),
    ?assertEqual({ok, true}, kura_test_repo:exists(Q)).

t_exists_false() ->
    Q = kura_query:where(
        kura_query:from(kura_test_schema), {email, <<"no_such_user_ever@example.com">>}
    ),
    ?assertEqual({ok, false}, kura_test_repo:exists(Q)).

t_reload() ->
    {ok, User} = insert_user(<<"ReloadUser">>, <<"reloaduser@example.com">>),
    %% Update via raw SQL to simulate external change
    kura_test_repo:query(
        "UPDATE \"users\" SET \"name\" = $1 WHERE \"id\" = $2",
        [<<"ReloadedName">>, maps:get(id, User)]
    ),
    {ok, Reloaded} = kura_test_repo:reload(kura_test_schema, User),
    ?assertEqual(<<"ReloadedName">>, maps:get(name, Reloaded)),
    ?assertEqual(maps:get(id, User), maps:get(id, Reloaded)).

t_reload_deleted() ->
    {ok, User} = insert_user(<<"ReloadDel">>, <<"reloaddel@example.com">>),
    CS = kura_changeset:cast(kura_test_schema, User, #{}, []),
    {ok, _} = kura_test_repo:delete(CS),
    ?assertEqual({error, not_found}, kura_test_repo:reload(kura_test_schema, User)).

t_reload_no_pk() ->
    ?assertEqual(
        {error, no_primary_key}, kura_test_repo:reload(kura_test_schema, #{name => <<"x">>})
    ).

%%----------------------------------------------------------------------
%% Subqueries
%%----------------------------------------------------------------------

t_subquery_in() ->
    {ok, User} = insert_user(<<"SubqUser">>, <<"subquser@example.com">>),
    %% Create a post referencing this user
    PostCS = kura_changeset:cast(
        kura_test_post_simple_schema,
        #{},
        #{<<"title">> => <<"Post1">>, <<"author_id">> => maps:get(id, User)},
        [title, author_id]
    ),
    {ok, _} = kura_test_repo:insert(PostCS),
    %% Subquery: find users who have posts
    SubQ = kura_query:select(kura_query:from(kura_test_post_simple_schema), [author_id]),
    Q = kura_query:where(
        kura_query:from(kura_test_schema),
        {'and', [{id, in, {subquery, SubQ}}, {email, <<"subquser@example.com">>}]}
    ),
    {ok, Results} = kura_test_repo:all(Q),
    ?assertEqual(1, length(Results)),
    ?assertEqual(<<"SubqUser">>, maps:get(name, hd(Results))).

t_exists_subquery() ->
    {ok, _} = insert_user(<<"ExSubUser">>, <<"exsubuser@example.com">>),
    SubQ = kura_query:where(
        kura_query:from(kura_test_schema), {email, <<"exsubuser@example.com">>}
    ),
    Q = kura_query:where(kura_query:from(kura_test_schema), {exists, {subquery, SubQ}}),
    {ok, Results} = kura_test_repo:all(Q),
    ?assert(length(Results) >= 1).

%%----------------------------------------------------------------------
%% CTEs
%%----------------------------------------------------------------------

t_cte() ->
    {ok, _} = insert_user(<<"CteAdmin">>, <<"cteadmin@example.com">>, #{
        <<"role">> => <<"cte_admin">>
    }),
    {ok, _} = insert_user(<<"CteUser">>, <<"cteuser@example.com">>, #{<<"role">> => <<"cte_user">>}),
    CteQ = kura_query:where(kura_query:from(kura_test_schema), {role, <<"cte_admin">>}),
    Q = kura_query:with_cte(kura_query:from(cte_admins), <<"cte_admins">>, CteQ),
    %% Note: cte_admins is not a real table, it's the CTE alias.
    %% But the query will use it as the FROM source.
    {ok, Results} = kura_test_repo:all(Q),
    ?assertEqual(1, length(Results)),
    ?assertEqual(<<"CteAdmin">>, maps:get(name, hd(Results))).

%%----------------------------------------------------------------------
%% UNION / INTERSECT / EXCEPT
%%----------------------------------------------------------------------

t_union() ->
    {ok, _} = insert_user(<<"UnionA">>, <<"uniona@example.com">>, #{<<"role">> => <<"union_a">>}),
    {ok, _} = insert_user(<<"UnionB">>, <<"unionb@example.com">>, #{<<"role">> => <<"union_b">>}),
    Q1 = kura_query:where(kura_query:from(kura_test_schema), {role, <<"union_a">>}),
    Q2 = kura_query:where(kura_query:from(kura_test_schema), {role, <<"union_b">>}),
    Q = kura_query:union(Q1, Q2),
    {ok, Results} = kura_test_repo:all(Q),
    Names = [maps:get(name, R) || R <- Results],
    ?assert(lists:member(<<"UnionA">>, Names)),
    ?assert(lists:member(<<"UnionB">>, Names)).

t_intersect() ->
    {ok, _} = insert_user(
        <<"IntUser">>, <<"intuser@example.com">>, #{
            <<"role">> => <<"int_role">>, <<"active">> => true
        }
    ),
    Q1 = kura_query:where(kura_query:from(kura_test_schema), {role, <<"int_role">>}),
    Q2 = kura_query:where(kura_query:from(kura_test_schema), {active, true}),
    Q = kura_query:intersect(Q1, Q2),
    {ok, Results} = kura_test_repo:all(Q),
    Names = [maps:get(name, R) || R <- Results],
    ?assert(lists:member(<<"IntUser">>, Names)).

t_except() ->
    {ok, _} = insert_user(
        <<"ExcA">>, <<"exca@example.com">>, #{<<"role">> => <<"exc_role">>, <<"active">> => true}
    ),
    {ok, _} = insert_user(
        <<"ExcB">>, <<"excb@example.com">>, #{<<"role">> => <<"exc_role">>, <<"active">> => false}
    ),
    Q1 = kura_query:where(kura_query:from(kura_test_schema), {role, <<"exc_role">>}),
    Q2 = kura_query:where(kura_query:from(kura_test_schema), {active, false}),
    Q = kura_query:except(Q1, Q2),
    {ok, Results} = kura_test_repo:all(Q),
    Names = [maps:get(name, R) || R <- Results],
    ?assert(lists:member(<<"ExcA">>, Names)),
    ?assertNot(lists:member(<<"ExcB">>, Names)).

%%----------------------------------------------------------------------
%% Window functions
%%----------------------------------------------------------------------

t_window_function() ->
    {ok, _} = insert_user(<<"WinA">>, <<"wina@example.com">>, #{<<"score">> => 10.0}),
    {ok, _} = insert_user(<<"WinB">>, <<"winb@example.com">>, #{<<"score">> => 20.0}),
    Q = kura_query:where(
        kura_query:select_expr(kura_query:from(kura_test_schema), [
            {row_num, {fragment, <<"ROW_NUMBER() OVER (ORDER BY \"score\" ASC)">>, []}},
            {user_name, {fragment, <<"\"name\"">>, []}}
        ]),
        {name, in, [<<"WinA">>, <<"WinB">>]}
    ),
    Q1 = kura_query:order_by(Q, [{row_num, asc}]),
    {ok, Results} = kura_test_repo:all(Q1),
    ?assertEqual(2, length(Results)),
    ?assertEqual(<<"WinA">>, maps:get(user_name, hd(Results))).

%%----------------------------------------------------------------------
%% insert_all with RETURNING
%%----------------------------------------------------------------------

t_insert_all_returning_true() ->
    Entries = [
        #{name => <<"RetA">>, email => <<"reta@example.com">>},
        #{name => <<"RetB">>, email => <<"retb@example.com">>}
    ],
    {ok, Count, Rows} = kura_test_repo:insert_all(kura_test_schema, Entries, #{returning => true}),
    ?assertEqual(2, Count),
    ?assertEqual(2, length(Rows)),
    ?assert(is_integer(maps:get(id, hd(Rows)))).

t_insert_all_returning_fields() ->
    Entries = [
        #{name => <<"RetFA">>, email => <<"retfa@example.com">>}
    ],
    {ok, Count, Rows} = kura_test_repo:insert_all(
        kura_test_schema, Entries, #{returning => [id, name]}
    ),
    ?assertEqual(1, Count),
    ?assertEqual(1, length(Rows)),
    Row = hd(Rows),
    ?assert(is_integer(maps:get(id, Row))),
    ?assertEqual(<<"RetFA">>, maps:get(name, Row)).

%%----------------------------------------------------------------------
%% prepare_changes
%%----------------------------------------------------------------------

t_prepare_changes_insert() ->
    CS = kura_changeset:cast(
        kura_test_schema,
        #{},
        #{<<"name">> => <<"PrepUser">>, <<"email">> => <<"prepuser@example.com">>},
        [name, email]
    ),
    CS1 = kura_changeset:validate_required(CS, [name, email]),
    CS2 = kura_changeset:prepare_changes(CS1, fun(C) ->
        kura_changeset:put_change(C, role, <<"prepared">>)
    end),
    {ok, User} = kura_test_repo:insert(CS2),
    ?assertEqual(<<"prepared">>, maps:get(role, User)).

%%----------------------------------------------------------------------
%% Optimistic locking
%%----------------------------------------------------------------------

t_optimistic_lock_ok() ->
    {ok, User} = insert_user(
        <<"LockOk">>, <<"lockok@example.com">>, #{<<"lock_version">> => 0}
    ),
    CS = kura_changeset:cast(
        kura_test_schema, User, #{<<"name">> => <<"LockOk Updated">>}, [name]
    ),
    CS1 = kura_changeset:optimistic_lock(CS, lock_version),
    {ok, Updated} = kura_test_repo:update(CS1),
    ?assertEqual(<<"LockOk Updated">>, maps:get(name, Updated)),
    ?assertEqual(1, maps:get(lock_version, Updated)).

t_optimistic_lock_stale() ->
    {ok, User} = insert_user(
        <<"LockStale">>, <<"lockstale@example.com">>, #{<<"lock_version">> => 0}
    ),
    %% Simulate a concurrent update by bumping lock_version directly
    kura_test_repo:query(
        "UPDATE \"users\" SET \"lock_version\" = 99 WHERE \"id\" = $1",
        [maps:get(id, User)]
    ),
    CS = kura_changeset:cast(
        kura_test_schema, User, #{<<"name">> => <<"Stale">>}, [name]
    ),
    CS1 = kura_changeset:optimistic_lock(CS, lock_version),
    ?assertEqual({error, stale}, kura_test_repo:update(CS1)).

%%----------------------------------------------------------------------
%% Streaming
%%----------------------------------------------------------------------

t_stream() ->
    %% Insert some test data
    Entries = [
        #{name => <<"StreamA">>, email => <<"streama@example.com">>},
        #{name => <<"StreamB">>, email => <<"streamb@example.com">>},
        #{name => <<"StreamC">>, email => <<"streamc@example.com">>}
    ],
    {ok, 3} = kura_test_repo:insert_all(kura_test_schema, Entries),
    Q = kura_query:where(
        kura_query:from(kura_test_schema),
        {name, in, [<<"StreamA">>, <<"StreamB">>, <<"StreamC">>]}
    ),
    Self = self(),
    ok = kura_stream:stream(
        kura_test_repo,
        Q,
        fun(Batch) ->
            Self ! {batch, Batch},
            ok
        end,
        #{batch_size => 2}
    ),
    %% Collect all batches
    Batches = collect_batches([]),
    AllRows = lists:flatten(Batches),
    Names = [maps:get(name, R) || R <- AllRows],
    ?assert(lists:member(<<"StreamA">>, Names)),
    ?assert(lists:member(<<"StreamB">>, Names)),
    ?assert(lists:member(<<"StreamC">>, Names)).

collect_batches(Acc) ->
    receive
        {batch, Batch} -> collect_batches([Batch | Acc])
    after 100 -> lists:reverse(Acc)
    end.

%%----------------------------------------------------------------------
%% Scopes
%%----------------------------------------------------------------------

t_scope() ->
    {ok, _} = insert_user(
        <<"ScopeUser">>,
        <<"scopeuser@example.com">>,
        #{<<"role">> => <<"scope_admin">>, <<"active">> => true}
    ),
    Active = fun(Q) -> kura_query:where(Q, {active, true}) end,
    Admin = fun(Q) -> kura_query:where(Q, {role, <<"scope_admin">>}) end,
    Q = kura_query:scope(kura_query:scope(kura_query:from(kura_test_schema), Active), Admin),
    {ok, Results} = kura_test_repo:all(Q),
    Names = [maps:get(name, R) || R <- Results],
    ?assert(lists:member(<<"ScopeUser">>, Names)).
