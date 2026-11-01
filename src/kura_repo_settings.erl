-module(kura_repo_settings).
-moduledoc false.

-export([lookup/1, from_config/1, started/2, refused/2, not_started/2, forget/1]).

-ifdef(TEST).
-export([forget/0]).
-endif.

-export_type([stored/0]).

-define(SETTINGS, [dialect, pool, pool_module, driver_module, read_only, replicas, uuid_version]).

-type stored() ::
    kura_repo:settings() | {pool_owned_by, module()} | {pool_not_started, atom()} | none.

%% Only this module writes these terms, and only in the shapes matched here.
-spec lookup(module()) -> stored().
lookup(Repo) ->
    case persistent_term:get({?MODULE, Repo}, none) of
        Settings when is_map(Settings) -> Settings;
        {pool_owned_by, Owner} when is_atom(Owner) -> {pool_owned_by, Owner};
        {pool_not_started, Pool} when is_atom(Pool) -> {pool_not_started, Pool};
        _None -> none
    end.

-spec from_config(map()) -> kura_repo:settings().
from_config(Config) ->
    maps:with(?SETTINGS, Config).

-spec started(module(), map()) -> ok.
started(Repo, Config) ->
    persistent_term:put({?MODULE, Repo}, from_config(Config)).

-spec refused(module(), module()) -> ok.
refused(Repo, Owner) ->
    persistent_term:put({?MODULE, Repo}, {pool_owned_by, Owner}).

-spec not_started(module(), atom()) -> ok.
not_started(Repo, Pool) ->
    persistent_term:put({?MODULE, Repo}, {pool_not_started, Pool}).

-spec forget(module()) -> ok.
forget(Repo) ->
    _ = persistent_term:erase({?MODULE, Repo}),
    ok.

-ifdef(TEST).
forget() ->
    _ = [persistent_term:erase(Key) || {{?MODULE, _} = Key, _} <- persistent_term:get()],
    ok.
-endif.
