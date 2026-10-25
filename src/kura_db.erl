-module(kura_db).
-moduledoc """
Central database wrapper. Routes all SQL through a configured
`kura_driver` impl over a configured `kura_pool` impl, so swapping
the underlying client (minato, esqlite, ...) is one config change.

All driver calls go through this module, providing a single place
for pool lookup, sandbox support, telemetry, and type loading.
""".

-export([
    query/3,
    transaction/2,
    transaction_ok/2,
    in_transaction/1,
    get_pool/1,
    get_pool_module/1,
    start_pool/3,
    get_driver_module/1,
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

-include_lib("kernel/include/logger.hrl").

-define(DECODE_OPTS, [return_rows_as_maps, column_name_as_atom]).

%% `key` is the TLS private key in ssl options.
-define(IS_SECRET_KEY(Key), (Key =:= password orelse Key =:= key)).

%%----------------------------------------------------------------------
%% Query
%%----------------------------------------------------------------------

-spec query(module(), iodata(), [term()]) -> map() | {error, term()}.
query(RepoMod, SQL, Params) ->
    Pool = get_pool(RepoMod),
    DriverMod = get_driver_module(RepoMod),
    T0 = erlang:monotonic_time(),
    Result =
        case kura_sandbox:get_conn(Pool) of
            {ok, Conn} ->
                DriverMod:query_on(Conn, SQL, Params, #{decode_opts => ?DECODE_OPTS});
            not_found ->
                PoolMod = get_pool_module(RepoMod),
                DriverMod:query(PoolMod, Pool, SQL, Params, #{decode_opts => ?DECODE_OPTS})
        end,
    T1 = erlang:monotonic_time(),
    DurationNative = T1 - T0,
    DurationUs = erlang:convert_time_unit(DurationNative, native, microsecond),
    emit_telemetry(RepoMod, SQL, Params, Result, DurationNative, DurationUs),
    Result.

%%----------------------------------------------------------------------
%% Transaction
%%----------------------------------------------------------------------

-doc """
Run `Fun` in a transaction on `RepoMod`'s pool, through its driver.

The driver's `c:kura_driver:transaction/4` decides what a nested call
does and what a failure raises. In a sandboxed test the sandbox
connection is already inside a transaction, so `Fun` runs under a
`SAVEPOINT` on it instead, the way a driver runs a nested transaction:
an exception rolls back to the savepoint and is re-raised, and a
savepoint the database will not release raises
`transaction_rolled_back` after rolling back to it. Another process
opening a transaction on the same sandbox connection while this one is
open raises `{kura_sandbox, overlapping_transactions}`.
""".
-spec transaction(module(), fun(() -> term())) -> term().
transaction(RepoMod, Fun) ->
    Pool = get_pool(RepoMod),
    DriverMod = get_driver_module(RepoMod),
    Nested = erlang:get({?MODULE, transaction, Pool}) =:= true,
    _ = erlang:put({?MODULE, transaction, Pool}, true),
    try
        case kura_sandbox:get_conn(Pool) of
            {ok, Conn} ->
                sandboxed(DriverMod, Conn, Fun);
            not_found ->
                PoolMod = get_pool_module(RepoMod),
                DriverMod:transaction(PoolMod, Pool, Fun, #{})
        end
    after
        left_transaction(Pool, Nested)
    end.

-doc """
Whether this process is inside `transaction/2` on `RepoMod`'s pool. A
process it spawns is not, and would not see the transaction's
uncommitted work.
""".
-spec in_transaction(module()) -> boolean().
in_transaction(RepoMod) ->
    erlang:get({?MODULE, transaction, get_pool(RepoMod)}) =:= true.

-spec left_transaction(atom(), boolean()) -> ok.
left_transaction(Pool, false) ->
    _ = erlang:erase({?MODULE, transaction, Pool}),
    ok;
left_transaction(_Pool, true) ->
    ok.

-spec sandboxed(module(), kura_pool:conn(), fun(() -> term())) -> term().
sandboxed(DriverMod, Conn, Fun) ->
    ok = kura_sandbox:enter_transaction(Conn),
    try
        savepointed(DriverMod, Conn, Fun)
    after
        ok = kura_sandbox:leave_transaction(Conn)
    end.

-spec savepointed(module(), kura_pool:conn(), fun(() -> term())) -> term().
savepointed(DriverMod, Conn, Fun) ->
    Savepoint = [
        ~"kura_savepoint_", integer_to_binary(erlang:unique_integer([positive, monotonic]))
    ],
    case on_sandbox(DriverMod, Conn, [~"SAVEPOINT ", Savepoint]) of
        {error, Refused} -> error({DriverMod, {savepoint_failed, reduced(Refused)}});
        _Started -> ok
    end,
    try Fun() of
        Value -> released(DriverMod, Conn, Savepoint, Value)
    catch
        Class:Reason:Stack ->
            ok = rolled_back_to(DriverMod, Conn, Savepoint),
            erlang:raise(Class, Reason, Stack)
    end.

-spec released(module(), kura_pool:conn(), iodata(), term()) -> term().
released(DriverMod, Conn, Savepoint, Value) ->
    case on_sandbox(DriverMod, Conn, [~"RELEASE SAVEPOINT ", Savepoint]) of
        {error, _} ->
            ok = rolled_back_to(DriverMod, Conn, Savepoint),
            error(transaction_rolled_back);
        _Released ->
            Value
    end.

-spec rolled_back_to(module(), kura_pool:conn(), iodata()) -> ok.
rolled_back_to(DriverMod, Conn, Savepoint) ->
    _ = on_sandbox(DriverMod, Conn, [~"ROLLBACK TO SAVEPOINT ", Savepoint]),
    _ = on_sandbox(DriverMod, Conn, [~"RELEASE SAVEPOINT ", Savepoint]),
    ok.

%% A server error's message and detail can carry row values.
-spec reduced(term()) -> term().
reduced({pgsql_error, Fields}) when is_map(Fields) ->
    {pgsql_error, maps:with([code, constraint, severity, severity_non_localised], Fields)};
reduced(Reason) ->
    Reason.

%% A raise here would replace the exception being cleaned up after.
-spec on_sandbox(module(), kura_pool:conn(), iodata()) -> dynamic().
on_sandbox(DriverMod, Conn, SQL) ->
    try
        DriverMod:query_on(Conn, SQL, [], #{})
    catch
        Class:_ -> {error, {raised, Class}}
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

-doc "Resolve the `kura_pool` implementation module for a repo.".
-spec get_pool_module(module()) -> module().
get_pool_module(RepoMod) ->
    Config = kura_repo:config(RepoMod),
    case maps:get(pool_module, Config, undefined) of
        M when is_atom(M), M =/= undefined -> M;
        _ -> error({no_pool_module_configured, RepoMod})
    end.

-doc false.
-spec start_pool(module(), atom(), map()) -> ok.
start_pool(Repo, Pool, Config) ->
    PoolMod = get_pool_module(Repo),
    Secrets = secrets(Config),
    try PoolMod:start_pool(Pool, Config) of
        {ok, _Pid} ->
            ok;
        {error, {already_started, _Pid}} ->
            ok;
        {error, Reason} ->
            not_started(Repo, PoolMod, #{reason => redacted(Reason, Secrets)})
    catch
        Class:Reason:Stack ->
            Redacted = {Class, redacted(Reason, Secrets)},
            ok = not_started(Repo, PoolMod, #{reason => Redacted, stacktrace => arities(Stack)}),
            error({pool_start_failed, Repo, Redacted})
    end.

-spec not_started(module(), module(), map()) -> ok.
not_started(Repo, PoolMod, Report) ->
    ?LOG_ERROR(Report#{msg => ~"kura: pool did not start", repo => Repo, pool_module => PoolMod}),
    ok.

%% A start that raises carries the config in its stacktrace arguments.
-type frame() ::
    {module(), atom(), arity() | [term()], [term()]} | {fun(), arity() | [term()], [term()]}.

-spec arities([frame()]) -> [frame()].
arities(Stack) ->
    [arity_frame(Frame) || Frame <- Stack].

-spec arity_frame(frame()) -> frame().
arity_frame({Module, Function, Args, Location}) -> {Module, Function, arity(Args), Location};
arity_frame({Fun, Args, Location}) -> {Fun, arity(Args), Location}.

-spec arity(arity() | [term()]) -> arity().
arity(Args) when is_list(Args) -> length(Args);
arity(Arity) -> Arity.

%% A supervisor reports a failed start with the child's start arguments, and
%% those hold the connection settings, password and TLS key included.
-spec secrets(map()) -> [term()].
secrets(Config) ->
    passwords(Config) ++ tls_keys(Config).

-spec tls_keys(map()) -> [term()].
tls_keys(#{ssl_options := Options}) when is_list(Options) ->
    [Form || {key, Key} <- Options, Form <- key_forms(Key)];
tls_keys(_Config) ->
    [].

-spec key_forms(term()) -> [term()].
key_forms({_Type, Der} = Key) when is_binary(Der) -> [Key, Der];
key_forms(Key) -> [Key].

-spec passwords(map()) -> [binary() | string()].
passwords(#{password := Password}) when is_binary(Password), Password =/= <<>> ->
    forms(Password);
passwords(#{password := Password}) when is_list(Password), Password =/= [] ->
    [
        Password
        | [
            Form
         || Binary <- [as_text(Password), as_bytes(Password)],
            is_binary(Binary),
            Form <- forms(Binary)
        ]
    ];
passwords(_Config) ->
    [].

-spec forms(binary()) -> [binary() | string()].
forms(Password) ->
    Text = [List || List <- [unicode:characters_to_list(Password)], is_list(List)],
    [Password, binary_to_list(Password) | Text].

-spec as_text(string()) -> binary() | undefined.
as_text(Password) ->
    case unicode:characters_to_binary(Password) of
        Binary when is_binary(Binary) -> Binary;
        _NotText -> undefined
    end.

%% A password given as a binary reaches some pools as a list of its bytes,
%% and is turned back into the binary by list_to_binary, not as text.
-spec as_bytes(string()) -> binary() | undefined.
as_bytes(Password) ->
    try
        list_to_binary(Password)
    catch
        error:badarg -> undefined
    end.

-spec redacted(term(), [term()]) -> term().
redacted(Term, Secrets) ->
    case lists:member(Term, Secrets) of
        true -> redacted;
        false -> redacted_parts(Term, Secrets)
    end.

-spec redacted_parts(term(), [term()]) -> term().
redacted_parts({Key, _}, _Secrets) when ?IS_SECRET_KEY(Key) ->
    {Key, redacted};
redacted_parts(Map, Secrets) when is_map(Map) ->
    #{redacted(K, Secrets) => redacted_value(K, V, Secrets) || K := V <- Map};
redacted_parts(Tuple, Secrets) when is_tuple(Tuple) ->
    list_to_tuple([redacted(E, Secrets) || E <- tuple_to_list(Tuple)]);
redacted_parts(List, Secrets) when is_list(List) ->
    try length(List) of
        _Proper -> [redacted(E, Secrets) || E <- List]
    catch
        error:badarg -> redacted
    end;
redacted_parts(Term, _Secrets) ->
    Term.

-spec redacted_value(term(), term(), [term()]) -> term().
redacted_value(Key, _Value, _Secrets) when ?IS_SECRET_KEY(Key) -> redacted;
redacted_value(_Key, Value, Secrets) -> redacted(Value, Secrets).

-doc "Resolve the `kura_driver` implementation module for a repo.".
-spec get_driver_module(module()) -> module().
get_driver_module(RepoMod) ->
    Config = kura_repo:config(RepoMod),
    case maps:get(driver_module, Config, undefined) of
        M when is_atom(M), M =/= undefined -> M;
        _ -> error({no_driver_module_configured, RepoMod})
    end.

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
