-module(kura_pool_ets).
-moduledoc """
ETS-backed `kura_pool` implementation. Hands out opaque connection
refs from an ETS-tracked free list. Useful for:

- Conformance tests against the `kura_pool` behaviour without docker.
- Unit tests of code that needs to lease/return connections but does
  not actually need to run SQL.
- A worked example for authors of new pool impls (the second impl is
  what proves a behaviour is real).

This pool does NOT speak any database protocol. The `conn()` value
returned from `checkout/2` is an opaque reference that callers can
match on in tests but cannot pass to `pgo` or any other driver.

## Pool layout

Each pool owns one ETS table, named after the pool atom. The table
holds two rows:

- `{available, [ref()]}` — free connection refs.
- `{checked_out, #{ref() => pid()}}` — leased refs and their owner.

`pool_size` from `start_pool/2` opts determines how many refs are
pre-generated. Defaults to 1.

## Example

```erlang
{ok, _Pid} = kura_pool_ets:start_pool(my_test_pool, #{pool_size => 4}),
{ok, Conn, Token} = kura_pool_ets:checkout(my_test_pool, #{}),
%% ... use Conn ...
ok = kura_pool_ets:checkin(my_test_pool, Token),
ok = kura_pool_ets:stop_pool(my_test_pool).
```
""".

-behaviour(kura_pool).

-export([
    start_pool/2,
    stop_pool/1,
    checkout/2,
    checkin/2,
    give_away/3,
    resize/2
]).

-export_type([conn/0]).

-doc "Opaque connection ref. Tests may pattern-match on it; production code must not.".
-type conn() :: reference().

%%----------------------------------------------------------------------
%% kura_pool callbacks
%%----------------------------------------------------------------------

-spec start_pool(kura_pool:name(), kura_pool:opts()) -> {ok, pid()} | {error, term()}.
start_pool(Name, Opts) ->
    Size = maps:get(pool_size, Opts, 1),
    case ets:whereis(Name) of
        undefined ->
            Tid = ets:new(Name, [named_table, public, set]),
            Refs = make_refs(Size),
            true = ets:insert(Tid, [{available, Refs}, {checked_out, #{}}]),
            {ok, ets_owner_pid(Tid)};
        _Existing ->
            {error, {already_started, ets_owner_pid(Name)}}
    end.

-spec stop_pool(kura_pool:name()) -> ok.
stop_pool(Name) ->
    case ets:whereis(Name) of
        undefined ->
            ok;
        _Tid ->
            true = ets:delete(Name),
            ok
    end.

-spec checkout(kura_pool:name(), kura_pool:checkout_opts()) ->
    {ok, kura_pool:conn(), kura_pool:token()} | {error, term()}.
checkout(Name, _Opts) ->
    case ets:whereis(Name) of
        undefined ->
            {error, no_pool};
        _Tid ->
            case ets:lookup(Name, available) of
                [{available, []}] ->
                    {error, no_conns};
                [{available, [Ref | Rest]}] ->
                    [{checked_out, Out}] = ets:lookup(Name, checked_out),
                    Out2 = Out#{Ref => self()},
                    true = ets:insert(Name, [{available, Rest}, {checked_out, Out2}]),
                    {ok, Ref, Ref};
                [] ->
                    {error, pool_corrupt}
            end
    end.

-spec checkin(kura_pool:name(), kura_pool:token()) -> ok.
checkin(Name, Token) ->
    case ets:whereis(Name) of
        undefined ->
            ok;
        _Tid ->
            [{available, Avail}] = ets:lookup(Name, available),
            [{checked_out, Out}] = ets:lookup(Name, checked_out),
            case maps:is_key(Token, Out) of
                true ->
                    Out2 = maps:remove(Token, Out),
                    true = ets:insert(Name, [{available, [Token | Avail]}, {checked_out, Out2}]),
                    ok;
                false ->
                    %% Idempotent: double-checkin is a no-op so callers using
                    %% with_conn-style finalizers do not crash on retry paths.
                    ok
            end
    end.

-spec give_away(kura_pool:token(), pid(), term()) -> ok | {error, term()}.
give_away(_Token, NewOwner, _GiftData) when is_pid(NewOwner) ->
    %% This pool tracks owner pids only loosely (for test introspection);
    %% the protocol semantics are upheld by checkin not crashing on a
    %% transferred token, which is the property test harnesses care about.
    ok;
give_away(_Token, _NotPid, _GiftData) ->
    {error, badarg}.

-spec resize(kura_pool:name(), pos_integer()) -> ok | {error, term()}.
resize(Name, Size) when is_integer(Size), Size > 0 ->
    case ets:whereis(Name) of
        undefined ->
            {error, no_pool};
        _Tid ->
            [{available, Avail}] = ets:lookup(Name, available),
            [{checked_out, Out}] = ets:lookup(Name, checked_out),
            Current = length(Avail) + map_size(Out),
            Avail2 =
                case Size - Current of
                    N when N > 0 -> make_refs(N) ++ Avail;
                    N when N < 0 -> drop_n(Avail, -N);
                    _ -> Avail
                end,
            true = ets:insert(Name, {available, Avail2}),
            ok
    end.

%%----------------------------------------------------------------------
%% Internal
%%----------------------------------------------------------------------

-spec make_refs(non_neg_integer()) -> [conn()].
make_refs(0) -> [];
make_refs(N) when N > 0 -> [erlang:make_ref() | make_refs(N - 1)].

-spec drop_n([conn()], non_neg_integer()) -> [conn()].
drop_n(L, 0) -> L;
drop_n([], _) -> [];
drop_n([_ | T], N) -> drop_n(T, N - 1).

%% ETS doesn't have a single owner-pid concept the same way a worker would;
%% this helper exists so start_pool can satisfy the {ok, pid()} contract.
%% We return whichever process owns the table.
-spec ets_owner_pid(atom() | ets:tid()) -> pid().
ets_owner_pid(NameOrTid) ->
    case ets:info(NameOrTid, owner) of
        Pid when is_pid(Pid) -> Pid;
        _ -> self()
    end.
