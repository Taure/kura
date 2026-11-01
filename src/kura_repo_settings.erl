-module(kura_repo_settings).
-moduledoc false.

-export([lookup/1, from_config/1, started/2, refused/2, taken/3]).

-ifdef(TEST).
-export([forget/0]).
-endif.

-export_type([stored/0]).

-define(SETTINGS, [dialect, pool, pool_module, driver_module, read_only, replicas, uuid_version]).

-type stored() :: kura_repo:settings() | {pool_owned_by, module()} | none.

-spec lookup(module()) -> stored().
lookup(Repo) ->
    stored(Repo).

-spec from_config(map()) -> kura_repo:settings().
from_config(Config) ->
    maps:with(?SETTINGS, Config).

-spec started(module(), map()) -> ok.
started(Repo, Config) ->
    persistent_term:put({?MODULE, Repo}, from_config(Config)).

-spec refused(module(), module()) -> ok.
refused(Repo, Owner) ->
    persistent_term:put({?MODULE, Repo}, {pool_owned_by, Owner}).

-spec taken(module(), atom(), module()) -> ok.
taken(Repo, Pool, Owner) ->
    case stored(Repo) of
        #{pool := Pool} -> refused(Repo, Owner);
        _Elsewhere -> ok
    end.

%% Only this module writes these terms.
-spec stored(module()) -> dynamic().
stored(Repo) ->
    persistent_term:get({?MODULE, Repo}, none).

-ifdef(TEST).
forget() ->
    _ = [persistent_term:erase(Key) || {{?MODULE, _} = Key, _} <- persistent_term:get()],
    ok.
-endif.
