-module(kura_repo_worker).

-include("kura.hrl").

-export([
    start/1,
    all/2,
    get/3,
    get_by/3,
    one/2,
    insert/2,
    insert/3,
    update/2,
    delete/2,
    update_all/3,
    delete_all/2,
    insert_all/3,
    transaction/2,
    query/3
]).

-define(DECODE_OPTS, [return_rows_as_maps, column_name_as_atom]).

%%----------------------------------------------------------------------
%% Pool management
%%----------------------------------------------------------------------

-spec start(module()) -> ok.
start(RepoMod) ->
    Config = RepoMod:config(),
    Pool = maps:get(pool, Config, RepoMod),
    PgoConfig = #{
        host => binary_to_list(maps:get(hostname, Config, <<"localhost">>)),
        port => maps:get(port, Config, 5432),
        database => binary_to_list(maps:get(database, Config)),
        user => binary_to_list(maps:get(username, Config, <<"postgres">>)),
        password => binary_to_list(maps:get(password, Config, <<>>)),
        pool_size => maps:get(pool_size, Config, 10),
        decode_opts => ?DECODE_OPTS
    },
    case pgo:start_pool(Pool, PgoConfig) of
        {ok, _Pid} -> ok;
        {error, {already_started, _Pid}} -> ok
    end.

%%----------------------------------------------------------------------
%% Query execution
%%----------------------------------------------------------------------

-spec all(module(), #kura_query{}) -> {ok, [map()]} | {error, term()}.
all(RepoMod, Query) ->
    {SQL, Params} = kura_query_compiler:to_sql(Query),
    maybe_log(SQL, Params),
    case pgo_query(RepoMod, SQL, Params) of
        #{command := select, rows := Rows} ->
            Schema = Query#kura_query.from,
            {ok, [load_row(Schema, Row) || Row <- Rows]};
        {error, _} = Err ->
            Err
    end.

-spec get(module(), module(), term()) -> {ok, map()} | {error, not_found} | {error, term()}.
get(RepoMod, SchemaMod, Id) ->
    PK = SchemaMod:primary_key(),
    Q = kura_query:where(kura_query:from(SchemaMod), {PK, Id}),
    case all(RepoMod, Q) of
        {ok, [Row]} -> {ok, Row};
        {ok, []} -> {error, not_found};
        {error, _} = Err -> Err
    end.

-spec get_by(module(), module(), [{atom(), term()}]) ->
    {ok, map()} | {error, not_found} | {error, term()}.
get_by(RepoMod, SchemaMod, Clauses) ->
    Q = lists:foldl(
        fun({Field, Value}, Acc) ->
            kura_query:where(Acc, {Field, Value})
        end,
        kura_query:from(SchemaMod),
        Clauses
    ),
    case all(RepoMod, Q) of
        {ok, [Row]} -> {ok, Row};
        {ok, []} -> {error, not_found};
        {ok, [_ | _]} -> {error, multiple_results};
        {error, _} = Err -> Err
    end.

-spec one(module(), #kura_query{}) -> {ok, map()} | {error, not_found} | {error, term()}.
one(RepoMod, Query) ->
    Q = kura_query:limit(Query, 1),
    case all(RepoMod, Q) of
        {ok, [Row]} -> {ok, Row};
        {ok, []} -> {error, not_found};
        {error, _} = Err -> Err
    end.

-spec insert(module(), #kura_changeset{}) -> {ok, map()} | {error, #kura_changeset{}}.
insert(_RepoMod, CS = #kura_changeset{valid = false}) ->
    {error, CS#kura_changeset{action = insert}};
insert(RepoMod, CS = #kura_changeset{schema = SchemaMod, changes = Changes}) ->
    Changes1 = maybe_add_timestamps(SchemaMod, Changes, insert),
    Fields = maps:keys(Changes1),
    DumpedChanges = dump_changes(SchemaMod, Changes1),
    {SQL, Params} = kura_query_compiler:insert(SchemaMod, Fields, DumpedChanges),
    maybe_log(SQL, Params),
    case pgo_query(RepoMod, SQL, Params) of
        #{command := insert, rows := [Row]} ->
            {ok, load_row(SchemaMod, Row)};
        {error, #{code := <<"23505">>, constraint := Constraint}} ->
            Field = constraint_to_field(Constraint),
            {error,
                kura_changeset:add_error(
                    CS#kura_changeset{action = insert}, Field, <<"has already been taken">>
                )};
        {error, #{code := <<"23503">>, constraint := Constraint}} ->
            Field = constraint_to_field(Constraint),
            {error,
                kura_changeset:add_error(
                    CS#kura_changeset{action = insert}, Field, <<"does not exist">>
                )};
        {error, #{code := <<"23502">>, column := Column}} ->
            Field = binary_to_atom(Column, utf8),
            {error,
                kura_changeset:add_error(
                    CS#kura_changeset{action = insert}, Field, <<"can't be blank">>
                )};
        {error, Reason} ->
            {error,
                kura_changeset:add_error(
                    CS#kura_changeset{action = insert}, base, format_error(Reason)
                )}
    end.

-spec insert(module(), #kura_changeset{}, map()) -> {ok, map()} | {error, #kura_changeset{}}.
insert(_RepoMod, CS = #kura_changeset{valid = false}, _Opts) ->
    {error, CS#kura_changeset{action = insert}};
insert(RepoMod, CS = #kura_changeset{schema = SchemaMod, changes = Changes}, Opts) ->
    Changes1 = maybe_add_timestamps(SchemaMod, Changes, insert),
    Fields = maps:keys(Changes1),
    DumpedChanges = dump_changes(SchemaMod, Changes1),
    {SQL, Params} = kura_query_compiler:insert(SchemaMod, Fields, DumpedChanges, Opts),
    maybe_log(SQL, Params),
    case pgo_query(RepoMod, SQL, Params) of
        #{command := insert, rows := [Row]} ->
            {ok, load_row(SchemaMod, Row)};
        #{command := insert, rows := []} ->
            {ok, kura_changeset:apply_changes(CS)};
        {error, #{code := <<"23505">>, constraint := Constraint}} ->
            Field = constraint_to_field(Constraint),
            {error,
                kura_changeset:add_error(
                    CS#kura_changeset{action = insert}, Field, <<"has already been taken">>
                )};
        {error, #{code := <<"23503">>, constraint := Constraint}} ->
            Field = constraint_to_field(Constraint),
            {error,
                kura_changeset:add_error(
                    CS#kura_changeset{action = insert}, Field, <<"does not exist">>
                )};
        {error, #{code := <<"23502">>, column := Column}} ->
            Field = binary_to_atom(Column, utf8),
            {error,
                kura_changeset:add_error(
                    CS#kura_changeset{action = insert}, Field, <<"can't be blank">>
                )};
        {error, Reason} ->
            {error,
                kura_changeset:add_error(
                    CS#kura_changeset{action = insert}, base, format_error(Reason)
                )}
    end.

-spec update(module(), #kura_changeset{}) -> {ok, map()} | {error, #kura_changeset{}}.
update(_RepoMod, CS = #kura_changeset{valid = false}) ->
    {error, CS#kura_changeset{action = update}};
update(RepoMod, CS = #kura_changeset{schema = SchemaMod, data = Data, changes = Changes}) ->
    case maps:size(Changes) of
        0 ->
            {ok, Data};
        _ ->
            PK = SchemaMod:primary_key(),
            PKValue = maps:get(PK, Data),
            Changes1 = maybe_add_timestamps(SchemaMod, Changes, update),
            Fields = maps:keys(Changes1),
            DumpedChanges = dump_changes(SchemaMod, Changes1),
            {SQL, Params} = kura_query_compiler:update(
                SchemaMod, Fields, DumpedChanges, {PK, PKValue}
            ),
            maybe_log(SQL, Params),
            case pgo_query(RepoMod, SQL, Params) of
                #{command := update, rows := [Row]} ->
                    {ok, load_row(SchemaMod, Row)};
                #{command := update, rows := []} ->
                    {error,
                        kura_changeset:add_error(
                            CS#kura_changeset{action = update}, base, <<"record not found">>
                        )};
                {error, #{code := <<"23505">>, constraint := Constraint}} ->
                    Field = constraint_to_field(Constraint),
                    {error,
                        kura_changeset:add_error(
                            CS#kura_changeset{action = update}, Field, <<"has already been taken">>
                        )};
                {error, #{code := <<"23503">>, constraint := Constraint}} ->
                    Field = constraint_to_field(Constraint),
                    {error,
                        kura_changeset:add_error(
                            CS#kura_changeset{action = update}, Field, <<"does not exist">>
                        )};
                {error, Reason} ->
                    {error,
                        kura_changeset:add_error(
                            CS#kura_changeset{action = update}, base, format_error(Reason)
                        )}
            end
    end.

-spec delete(module(), #kura_changeset{}) -> {ok, map()} | {error, term()}.
delete(RepoMod, #kura_changeset{schema = SchemaMod, data = Data}) ->
    delete_record(RepoMod, SchemaMod, Data).

-spec update_all(module(), #kura_query{}, map()) -> {ok, non_neg_integer()} | {error, term()}.
update_all(RepoMod, Query, Updates) ->
    {SQL, Params} = kura_query_compiler:update_all(Query, Updates),
    maybe_log(SQL, Params),
    case pgo_query(RepoMod, SQL, Params) of
        #{command := update, num_rows := Count} -> {ok, Count};
        {error, _} = Err -> Err
    end.

-spec delete_all(module(), #kura_query{}) -> {ok, non_neg_integer()} | {error, term()}.
delete_all(RepoMod, Query) ->
    {SQL, Params} = kura_query_compiler:delete_all(Query),
    maybe_log(SQL, Params),
    case pgo_query(RepoMod, SQL, Params) of
        #{command := delete, num_rows := Count} -> {ok, Count};
        {error, _} = Err -> Err
    end.

-spec insert_all(module(), module(), [map()]) -> {ok, non_neg_integer()} | {error, term()}.
insert_all(RepoMod, SchemaMod, Entries) ->
    NonVirtual = kura_schema:non_virtual_fields(SchemaMod),
    Rows = [
        begin
            WithTS = maybe_add_timestamps(SchemaMod, Entry, insert),
            Filtered = maps:with(NonVirtual, WithTS),
            dump_changes(SchemaMod, Filtered)
        end
     || Entry <- Entries
    ],
    Fields = maps:keys(hd(Rows)),
    {SQL, Params} = kura_query_compiler:insert_all(SchemaMod, Fields, Rows),
    maybe_log(SQL, Params),
    case pgo_query(RepoMod, SQL, Params) of
        #{command := insert, num_rows := Count} -> {ok, Count};
        {error, _} = Err -> Err
    end.

-spec transaction(module(), fun(() -> any())) -> any() | {error, any()}.
transaction(RepoMod, Fun) ->
    Pool = get_pool(RepoMod),
    pgo:transaction(Fun, #{pool => Pool}).

-spec query(module(), iodata(), [term()]) -> {ok, list()} | {error, term()}.
query(RepoMod, SQL, Params) ->
    maybe_log(SQL, Params),
    case pgo_query(RepoMod, SQL, Params) of
        #{rows := Rows} -> {ok, Rows};
        {error, _} = Err -> Err
    end.

%%----------------------------------------------------------------------
%% Internal: pgo bridge
%%----------------------------------------------------------------------

pgo_query(RepoMod, SQL, Params) ->
    Pool = get_pool(RepoMod),
    pgo:query(SQL, Params, #{pool => Pool, decode_opts => ?DECODE_OPTS}).

get_pool(RepoMod) ->
    Config = RepoMod:config(),
    maps:get(pool, Config, RepoMod).

delete_record(RepoMod, SchemaMod, Data) ->
    PK = SchemaMod:primary_key(),
    PKValue = maps:get(PK, Data),
    {SQL, Params} = kura_query_compiler:delete(SchemaMod, PK, PKValue),
    maybe_log(SQL, Params),
    case pgo_query(RepoMod, SQL, Params) of
        #{command := delete, rows := [Row]} ->
            {ok, load_row(SchemaMod, Row)};
        #{command := delete, rows := []} ->
            {error, not_found};
        {error, _} = Err ->
            Err
    end.

load_row(SchemaMod, Row) when is_atom(SchemaMod) ->
    case code:ensure_loaded(SchemaMod) of
        {module, SchemaMod} ->
            case erlang:function_exported(SchemaMod, fields, 0) of
                true ->
                    Types = kura_schema:field_types(SchemaMod),
                    maps:fold(
                        fun(K, V, Acc) ->
                            case maps:find(K, Types) of
                                {ok, Type} ->
                                    case kura_types:load(Type, V) of
                                        {ok, Loaded} -> Acc#{K => Loaded};
                                        {error, _} -> Acc#{K => V}
                                    end;
                                error ->
                                    Acc#{K => V}
                            end
                        end,
                        #{},
                        Row
                    );
                false ->
                    Row
            end;
        _ ->
            Row
    end.

dump_changes(SchemaMod, Changes) ->
    Types = kura_schema:field_types(SchemaMod),
    NonVirtual = kura_schema:non_virtual_fields(SchemaMod),
    maps:fold(
        fun(K, V, Acc) ->
            case lists:member(K, NonVirtual) of
                false ->
                    Acc;
                true ->
                    case maps:find(K, Types) of
                        {ok, Type} ->
                            case kura_types:dump(Type, V) of
                                {ok, Dumped} -> Acc#{K => Dumped};
                                {error, _} -> Acc#{K => V}
                            end;
                        error ->
                            Acc#{K => V}
                    end
            end
        end,
        #{},
        Changes
    ).

maybe_add_timestamps(SchemaMod, Changes, Action) ->
    Types = kura_schema:field_types(SchemaMod),
    Now = calendar:universal_time(),
    Changes1 =
        case Action of
            insert ->
                case maps:is_key(inserted_at, Types) of
                    true -> Changes#{inserted_at => maps:get(inserted_at, Changes, Now)};
                    false -> Changes
                end;
            update ->
                Changes
        end,
    case maps:is_key(updated_at, Types) of
        true -> Changes1#{updated_at => maps:get(updated_at, Changes1, Now)};
        false -> Changes1
    end.

constraint_to_field(Constraint) when is_binary(Constraint) ->
    case binary:split(Constraint, <<"_">>, [global]) of
        [_Table, Field | _Rest] ->
            try
                binary_to_existing_atom(Field, utf8)
            catch
                error:badarg -> binary_to_atom(Field, utf8)
            end;
        _ ->
            base
    end;
constraint_to_field(_) ->
    base.

format_error(#{message := Msg}) when is_binary(Msg) -> Msg;
format_error(Reason) -> iolist_to_binary(io_lib:format("~p", [Reason])).

maybe_log(SQL, Params) ->
    case application:get_env(kura, log_queries, false) of
        true ->
            logger:debug("Kura SQL: ~s ~p", [SQL, Params]);
        false ->
            ok
    end.
