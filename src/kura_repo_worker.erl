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
    multi/2,
    preload/4,
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
            Loaded = [load_row(Schema, Row) || Row <- Rows],
            case Query#kura_query.preloads of
                [] -> {ok, Loaded};
                Preloads -> {ok, do_preload(RepoMod, Loaded, Schema, Preloads)}
            end;
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
        {error, PgError} ->
            {error, handle_pg_error(CS#kura_changeset{action = insert}, PgError)}
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
        {error, PgError} ->
            {error, handle_pg_error(CS#kura_changeset{action = insert}, PgError)}
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
                {error, PgError} ->
                    {error, handle_pg_error(CS#kura_changeset{action = update}, PgError)}
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

-spec multi(module(), term()) ->
    {ok, map()} | {error, atom(), term(), map()}.
multi(RepoMod, Multi) ->
    Ops = kura_multi:to_list(Multi),
    transaction(RepoMod, fun() ->
        execute_multi_ops(RepoMod, Ops, #{})
    end).

-spec query(module(), iodata(), [term()]) -> {ok, list()} | {error, term()}.
query(RepoMod, SQL, Params) ->
    maybe_log(SQL, Params),
    case pgo_query(RepoMod, SQL, Params) of
        #{rows := Rows} -> {ok, Rows};
        {error, _} = Err -> Err
    end.

%%----------------------------------------------------------------------
%% Preloading
%%----------------------------------------------------------------------

-spec preload(module(), module(), map() | [map()], [atom() | {atom(), list()}]) ->
    map() | [map()].
preload(RepoMod, Schema, Record, Assocs) when is_map(Record) ->
    [Result] = preload(RepoMod, Schema, [Record], Assocs),
    Result;
preload(_RepoMod, _Schema, [], _Assocs) ->
    [];
preload(RepoMod, Schema, Records, Assocs) when is_list(Records) ->
    do_preload(RepoMod, Records, Schema, Assocs).

do_preload(_RepoMod, Records, _Schema, []) ->
    Records;
do_preload(RepoMod, Records, Schema, [Assoc | Rest]) ->
    Updated = preload_assoc(RepoMod, Records, Schema, Assoc),
    do_preload(RepoMod, Updated, Schema, Rest).

preload_assoc(RepoMod, Records, Schema, {AssocName, Nested}) ->
    Records1 = preload_assoc(RepoMod, Records, Schema, AssocName),
    {ok, Assoc} = kura_schema:association(Schema, AssocName),
    RelatedSchema = Assoc#kura_assoc.schema,
    lists:map(
        fun(R) ->
            case maps:get(AssocName, R, undefined) of
                undefined ->
                    R;
                nil ->
                    R;
                Related when is_list(Related) ->
                    R#{AssocName => do_preload(RepoMod, Related, RelatedSchema, Nested)};
                Related when is_map(Related) ->
                    [Updated] = do_preload(RepoMod, [Related], RelatedSchema, Nested),
                    R#{AssocName => Updated}
            end
        end,
        Records1
    );
preload_assoc(RepoMod, Records, Schema, AssocName) when is_atom(AssocName) ->
    {ok, Assoc} = kura_schema:association(Schema, AssocName),
    case Assoc#kura_assoc.type of
        belongs_to -> preload_belongs_to(RepoMod, Records, Assoc);
        has_many -> preload_has_many(RepoMod, Records, Schema, Assoc);
        has_one -> preload_has_one(RepoMod, Records, Schema, Assoc)
    end.

preload_belongs_to(RepoMod, Records, #kura_assoc{
    name = Name, schema = RelSchema, foreign_key = FK
}) ->
    FKValues = lists:usort([
        maps:get(FK, R)
     || R <- Records, maps:get(FK, R, undefined) =/= undefined
    ]),
    case FKValues of
        [] ->
            [R#{Name => nil} || R <- Records];
        _ ->
            RelPK = RelSchema:primary_key(),
            Q = kura_query:where(kura_query:from(RelSchema), {RelPK, in, FKValues}),
            {ok, Related} = all(RepoMod, Q),
            Lookup = maps:from_list([{maps:get(RelPK, Rel), Rel} || Rel <- Related]),
            [R#{Name => maps:get(maps:get(FK, R, undefined), Lookup, nil)} || R <- Records]
    end.

preload_has_many(RepoMod, Records, Schema, #kura_assoc{
    name = Name, schema = RelSchema, foreign_key = FK
}) ->
    PK = Schema:primary_key(),
    PKValues = lists:usort([maps:get(PK, R) || R <- Records]),
    Q = kura_query:where(kura_query:from(RelSchema), {FK, in, PKValues}),
    {ok, Related} = all(RepoMod, Q),
    Grouped = lists:foldl(
        fun(Rel, Acc) ->
            Key = maps:get(FK, Rel),
            maps:update_with(Key, fun(L) -> L ++ [Rel] end, [Rel], Acc)
        end,
        #{},
        Related
    ),
    [R#{Name => maps:get(maps:get(PK, R), Grouped, [])} || R <- Records].

preload_has_one(RepoMod, Records, Schema, #kura_assoc{
    name = Name, schema = RelSchema, foreign_key = FK
}) ->
    PK = Schema:primary_key(),
    PKValues = lists:usort([maps:get(PK, R) || R <- Records]),
    Q = kura_query:where(kura_query:from(RelSchema), {FK, in, PKValues}),
    {ok, Related} = all(RepoMod, Q),
    Lookup = maps:from_list([{maps:get(FK, Rel), Rel} || Rel <- Related]),
    [R#{Name => maps:get(maps:get(PK, R), Lookup, nil)} || R <- Records].

%%----------------------------------------------------------------------
%% Internal: multi execution
%%----------------------------------------------------------------------

execute_multi_ops(_RepoMod, [], Acc) ->
    {ok, Acc};
execute_multi_ops(RepoMod, [{Name, Op} | Rest], Acc) ->
    case execute_multi_op(RepoMod, Op, Acc) of
        {ok, Result} ->
            execute_multi_ops(RepoMod, Rest, Acc#{Name => Result});
        {error, Value} ->
            pgo:break({error, Name, Value, Acc})
    end.

execute_multi_op(RepoMod, {insert, Fun}, Acc) when is_function(Fun, 1) ->
    execute_multi_op(RepoMod, {insert, Fun(Acc)}, Acc);
execute_multi_op(RepoMod, {insert, CS}, _Acc) ->
    insert(RepoMod, CS);
execute_multi_op(RepoMod, {update, Fun}, Acc) when is_function(Fun, 1) ->
    execute_multi_op(RepoMod, {update, Fun(Acc)}, Acc);
execute_multi_op(RepoMod, {update, CS}, _Acc) ->
    update(RepoMod, CS);
execute_multi_op(RepoMod, {delete, Fun}, Acc) when is_function(Fun, 1) ->
    execute_multi_op(RepoMod, {delete, Fun(Acc)}, Acc);
execute_multi_op(RepoMod, {delete, CS}, _Acc) ->
    delete(RepoMod, CS);
execute_multi_op(_RepoMod, {run, Fun}, Acc) ->
    Fun(Acc).

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

handle_pg_error(CS, #{code := Code, constraint := Constraint}) when
    Code =:= <<"23505">>; Code =:= <<"23503">>; Code =:= <<"23514">>
->
    DefaultType =
        case Code of
            <<"23505">> -> unique;
            <<"23503">> -> foreign_key;
            <<"23514">> -> check
        end,
    DefaultMsg =
        case DefaultType of
            unique -> <<"has already been taken">>;
            foreign_key -> <<"does not exist">>;
            check -> <<"is invalid">>
        end,
    case find_constraint(CS#kura_changeset.constraints, Constraint) of
        {ok, #kura_constraint{field = Field, message = Msg}} ->
            kura_changeset:add_error(CS, Field, Msg);
        error ->
            Field = constraint_to_field(Constraint),
            kura_changeset:add_error(CS, Field, DefaultMsg)
    end;
handle_pg_error(CS, #{code := <<"23502">>, column := Column}) ->
    Field = binary_to_atom(Column, utf8),
    kura_changeset:add_error(CS, Field, <<"can't be blank">>);
handle_pg_error(CS, Reason) ->
    kura_changeset:add_error(CS, base, format_error(Reason)).

find_constraint([], _Name) ->
    error;
find_constraint([C = #kura_constraint{constraint = Name} | _], Name) ->
    {ok, C};
find_constraint([_ | Rest], Name) ->
    find_constraint(Rest, Name).

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
