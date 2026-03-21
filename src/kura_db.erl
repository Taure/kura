-module(kura_db).
-moduledoc """
Central database wrapper for all pgo interactions.

All pgo calls go through this module, providing a single place for
pool lookup, sandbox support, telemetry, and type loading.
""".

-export([
    query/3,
    transaction/2,
    transaction_ok/2,
    get_pool/1,
    load_row/2,
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

%%----------------------------------------------------------------------
%% Query
%%----------------------------------------------------------------------

-spec query(module(), iodata(), [term()]) -> map() | {error, term()}.
query(RepoMod, SQL, Params) ->
    Pool = get_pool(RepoMod),
    T0 = erlang:monotonic_time(),
    Result =
        case kura_sandbox:get_conn(Pool) of
            {ok, Conn} ->
                pgo:query(SQL, Params, #{decode_opts => ?DECODE_OPTS}, Conn);
            not_found ->
                pgo:query(SQL, Params, #{pool => Pool, decode_opts => ?DECODE_OPTS})
        end,
    T1 = erlang:monotonic_time(),
    DurationNative = T1 - T0,
    DurationUs = erlang:convert_time_unit(DurationNative, native, microsecond),
    emit_telemetry(RepoMod, SQL, Params, Result, DurationNative, DurationUs),
    Result.

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
            pgo:transaction(Fun, #{pool => Pool})
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
    {ResultStatus, NumRows} =
        case Result of
            #{rows := Rows} -> {ok, length(Rows)};
            #{num_rows := N} -> {ok, N};
            {error, _} -> {error, 0};
            _ -> {ok, 0}
        end,
    Source = extract_source(SQLBin),
    #{
        query => SQLBin,
        params => Params,
        repo => RepoMod,
        result => ResultStatus,
        num_rows => NumRows,
        source => Source
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
