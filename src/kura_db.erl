-module(kura_db).
-moduledoc """
Central database wrapper for all driver interactions.

All driver calls go through this module, providing a single place for
pool lookup, sandbox support, telemetry, and type loading.
""".

-include_lib("epgsql/include/epgsql.hrl").

-export([
    query/3,
    query_pool/3,
    transaction/2,
    transaction_pool/2,
    transaction_ok/2,
    get_pool/1,
    load_row/2,
    translate_result/2,
    build_log_event/5,
    build_telemetry_metadata/4,
    extract_source/1,
    default_logger/0
]).

-ifdef(TEST).
-export([
    narrow_ok_error/1,
    extract_result_status/1,
    extract_num_rows/1,
    emit_telemetry/6
]).
-endif.

-define(DECODE_OPTS, [return_rows_as_maps, column_name_as_atom]).
-define(POOL_IMPL, kura_pool_hnc).
-define(TX_CONN_KEY, {?MODULE, tx_conn}).

%%----------------------------------------------------------------------
%% Query
%%----------------------------------------------------------------------

-spec query(module(), iodata(), [term()]) -> map() | {error, term()}.
query(RepoMod, SQL, Params) ->
    Pool = get_pool(RepoMod),
    T0 = erlang:monotonic_time(),
    Result = run_query(Pool, SQL, Params),
    T1 = erlang:monotonic_time(),
    DurationNative = T1 - T0,
    DurationUs = erlang:convert_time_unit(DurationNative, native, microsecond),
    emit_telemetry(RepoMod, SQL, Params, Result, DurationNative, DurationUs),
    Result.

run_query(Pool, SQL, Params) ->
    case kura_sandbox:get_conn(Pool) of
        {ok, SandboxConn} ->
            translate_result(epgsql:equery(SandboxConn, SQL, Params), SQL);
        not_found ->
            case get(?TX_CONN_KEY) of
                {Conn, Pool} ->
                    translate_result(epgsql:equery(Conn, SQL, Params), SQL);
                _ ->
                    kura_pool:with_conn(?POOL_IMPL, Pool, fun(Worker) ->
                        run_on_worker(Worker, SQL, Params)
                    end)
            end
    end.

run_on_worker(Worker, SQL, Params) ->
    case kura_pg_conn:get_conn(Worker) of
        {ok, Conn} -> translate_result(epgsql:equery(Conn, SQL, Params), SQL);
        {error, _} = Err -> Err
    end.

-doc false.
-spec query_pool(atom(), iodata(), [term()]) -> map() | {error, term()}.
query_pool(Pool, SQL, Params) ->
    case get(?TX_CONN_KEY) of
        {Conn, Pool} ->
            translate_result(epgsql:equery(Conn, SQL, Params), SQL);
        _ ->
            kura_pool:with_conn(?POOL_IMPL, Pool, fun(Worker) ->
                run_on_worker(Worker, SQL, Params)
            end)
    end.

-doc false.
-spec transaction_pool(atom(), fun(() -> term())) -> term().
transaction_pool(Pool, Fun) ->
    case get(?TX_CONN_KEY) of
        {_Conn, Pool} ->
            Fun();
        _ ->
            run_transaction(Pool, Fun)
    end.

%%----------------------------------------------------------------------
%% Transaction
%%----------------------------------------------------------------------

-spec transaction(module(), fun(() -> term())) -> term().
transaction(RepoMod, Fun) ->
    Pool = get_pool(RepoMod),
    case kura_sandbox:get_conn(Pool) of
        {ok, _Conn} ->
            Fun();
        not_found ->
            case get(?TX_CONN_KEY) of
                {_Conn, Pool} ->
                    Fun();
                _ ->
                    run_transaction(Pool, Fun)
            end
    end.

run_transaction(Pool, Fun) ->
    kura_pool:with_conn(?POOL_IMPL, Pool, fun(Worker) ->
        case kura_pg_conn:get_conn(Worker) of
            {ok, Conn} ->
                run_in_tx(Conn, Pool, Fun);
            {error, Reason} ->
                {error, Reason}
        end
    end).

run_in_tx(Conn, Pool, Fun) ->
    put(?TX_CONN_KEY, {Conn, Pool}),
    try
        {ok, [], []} = epgsql:squery(Conn, ~"BEGIN"),
        try Fun() of
            Result ->
                {ok, [], []} = epgsql:squery(Conn, ~"COMMIT"),
                Result
        catch
            Class:Reason:Stack ->
                _ = epgsql:squery(Conn, ~"ROLLBACK"),
                erlang:raise(Class, Reason, Stack)
        end
    after
        erase(?TX_CONN_KEY)
    end.

-spec transaction_ok(module(), fun(() -> term())) -> {ok, term()} | {error, term()}.
transaction_ok(RepoMod, Fun) ->
    narrow_ok_error(transaction(RepoMod, Fun)).

%%----------------------------------------------------------------------
%% Pool
%%----------------------------------------------------------------------

-spec get_pool(module()) -> atom().
get_pool(RepoMod) ->
    Config = kura_repo:config(RepoMod),
    maps:get(pool, Config, RepoMod).

%%----------------------------------------------------------------------
%% Row loading
%%----------------------------------------------------------------------

-spec load_row(module(), map()) -> map().
load_row(SchemaMod, Row) when is_atom(SchemaMod) ->
    case code:ensure_loaded(SchemaMod) of
        {module, SchemaMod} ->
            case erlang:function_exported(SchemaMod, fields, 0) of
                true ->
                    Types = kura_schema:field_types(SchemaMod),
                    maps:fold(
                        fun(K, V, Acc) ->
                            case Types of
                                #{K := Type} ->
                                    case kura_types:load(Type, V) of
                                        {ok, Loaded} -> Acc#{K => Loaded};
                                        {error, _} -> Acc#{K => V}
                                    end;
                                #{} ->
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

%%----------------------------------------------------------------------
%% Telemetry
%%----------------------------------------------------------------------

emit_telemetry(RepoMod, SQL, Params, Result, DurationNative, DurationUs) ->
    try
        Measurements = #{
            duration => DurationNative,
            duration_us => DurationUs
        },
        Metadata = build_telemetry_metadata(RepoMod, SQL, Params, Result),
        telemetry:execute([kura, repo, query], Measurements, Metadata),
        emit_legacy_log(RepoMod, SQL, Params, Result, DurationUs)
    catch
        _:_ -> ok
    end.

emit_legacy_log(RepoMod, SQL, Params, Result, DurationUs) ->
    case application:get_env(kura, log) of
        {ok, true} ->
            Event = build_log_event(RepoMod, SQL, Params, Result, DurationUs),
            (default_logger())(Event);
        {ok, LogFun} when is_function(LogFun, 1) ->
            Event = build_log_event(RepoMod, SQL, Params, Result, DurationUs),
            LogFun(Event);
        {ok, {M, F}} when is_atom(M), is_atom(F) ->
            Event = build_log_event(RepoMod, SQL, Params, Result, DurationUs),
            M:F(Event);
        _ ->
            ok
    end.

build_telemetry_metadata(RepoMod, SQL, Params, Result) ->
    SQLBin = iolist_to_binary(SQL),
    {ResultStatus, NumRows, ErrorReason} =
        case Result of
            #{rows := Rows} -> {ok, length(Rows), undefined};
            #{num_rows := N} -> {ok, N, undefined};
            {error, Reason} -> {error, 0, Reason};
            _ -> {ok, 0, undefined}
        end,
    Source = extract_source(SQLBin),
    #{
        query => SQLBin,
        params => Params,
        repo => RepoMod,
        result => ResultStatus,
        num_rows => NumRows,
        source => Source,
        tenant => kura_tenant:get_tenant(),
        error_reason => ErrorReason
    }.

extract_source(SQL) ->
    case
        re:run(SQL, ~"(?:FROM|INTO|UPDATE|TABLE)\\s+\"?(\\w+)\"?", [
            {capture, [1], binary}, caseless
        ])
    of
        {match, [Table]} -> Table;
        nomatch -> undefined
    end.

build_log_event(Repo, SQL, Params, Result, DurationUs) ->
    ResultStatus = extract_result_status(Result),
    NumRows = extract_num_rows(Result),
    #{
        query => iolist_to_binary([SQL]),
        params => Params,
        result => ResultStatus,
        num_rows => NumRows,
        duration_us => DurationUs,
        repo => Repo
    }.

default_logger() ->
    fun(Event) ->
        logger:info("Kura ~s ~pus", [
            maps:get(query, Event), maps:get(duration_us, Event)
        ])
    end.

%%----------------------------------------------------------------------
%% Type narrowing
%%----------------------------------------------------------------------

-spec narrow_ok_error(term()) -> {ok, term()} | {error, term()}.
narrow_ok_error({ok, _} = Ok) -> Ok;
narrow_ok_error({error, _} = Err) -> Err.

-spec extract_result_status(term()) -> ok | error.
extract_result_status({error, _}) -> error;
extract_result_status(_) -> ok.

-spec extract_num_rows(term()) -> non_neg_integer().
extract_num_rows(#{num_rows := N}) when is_integer(N) -> N;
extract_num_rows(#{rows := Rows}) when is_list(Rows) -> length(Rows);
extract_num_rows(_) -> 0.

%%----------------------------------------------------------------------
%% Result translation (epgsql -> kura/pgo result shape)
%%----------------------------------------------------------------------

-doc false.
-spec translate_result(term(), iodata()) -> map() | {error, term()}.
translate_result({ok, Cols, Rows}, SQL) ->
    #{
        command => command_from_sql(SQL),
        num_rows => length(Rows),
        rows => rows_as_maps(Cols, Rows)
    };
translate_result({ok, Count}, SQL) when is_integer(Count) ->
    #{
        command => command_from_sql(SQL),
        num_rows => Count,
        rows => []
    };
translate_result({ok, Count, Cols, Rows}, SQL) ->
    #{
        command => command_from_sql(SQL),
        num_rows => Count,
        rows => rows_as_maps(Cols, Rows)
    };
translate_result({error, #error{} = E}, _SQL) ->
    {error, translate_error(E)};
translate_result({error, _} = Err, _SQL) ->
    Err.

rows_as_maps(_Cols, []) ->
    [];
rows_as_maps(Cols, Rows) ->
    Specs = [{binary_to_atom(C#column.name, utf8), C#column.type} || C <- Cols],
    [row_to_map(Specs, R) || R <- Rows].

row_to_map(Specs, Row) ->
    maps:from_list(
        [{N, decode_value(T, V)} || {{N, T}, V} <- lists:zip(Specs, tuple_to_list(Row))]
    ).

decode_value(_, null) ->
    null;
decode_value(numeric, V) ->
    parse_numeric_text(V);
decode_value({unknown_oid, 1700}, V) ->
    parse_numeric_text(V);
decode_value(Ts, {{_, _, _} = D, {H, Mi, S}}) when
    Ts =:= timestamp; Ts =:= timestamptz
->
    {D, {H, Mi, normalize_seconds(S)}};
decode_value(T, {H, Mi, S}) when
    T =:= time orelse T =:= timetz, is_integer(H), is_integer(Mi)
->
    {H, Mi, normalize_seconds(S)};
decode_value(_, V) ->
    V.

parse_numeric_text(V) when is_binary(V) ->
    case string:to_float(V) of
        {F, <<>>} when is_float(F) ->
            F;
        _ ->
            case string:to_integer(V) of
                {I, <<>>} when is_integer(I) -> I;
                _ -> V
            end
    end;
parse_numeric_text(V) ->
    V.

normalize_seconds(S) when is_integer(S) ->
    S;
normalize_seconds(S) when is_float(S) ->
    case S - trunc(S) of
        +0.0 -> trunc(S);
        _ -> S
    end.

command_from_sql(SQL) ->
    Lower = string:lowercase(iolist_to_binary(SQL)),
    case
        re:run(
            Lower,
            ~"^\\s*with\\s.*?\\)\\s+(select|insert|update|delete)\\b",
            [{capture, [1], binary}, dotall]
        )
    of
        {match, [Cmd]} ->
            binary_to_atom(Cmd, utf8);
        nomatch ->
            case re:run(Lower, ~"^\\s*(\\w+)", [{capture, [1], binary}]) of
                {match, [Cmd]} -> binary_to_atom(Cmd, utf8);
                nomatch -> unknown
            end
    end.

translate_error(#error{
    severity = Sev, code = Code, codename = CodeName, message = Msg, extra = Extra
}) ->
    Base = #{severity => Sev, code => Code, codename => CodeName, message => Msg},
    Translated = [translate_extra_field(KV) || KV <- Extra],
    maps:merge(Base, maps:from_list(Translated)).

translate_extra_field({constraint_name, V}) -> {constraint, V};
translate_extra_field({column_name, V}) -> {column, V};
translate_extra_field({detail_message, V}) -> {detail, V};
translate_extra_field(KV) -> KV.
