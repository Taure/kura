-module(kura_repo_settings).
-moduledoc false.

-export([lookup/1, from_config/1, started/2, refused/2, not_started/2, forget/1]).

-ifdef(TEST).
-export([forget/0]).
-endif.

-export_type([stored/0]).

-define(SETTINGS, [
    dialect,
    pool,
    pool_module,
    driver_module,
    read_only,
    replicas,
    uuid_version,
    require_tenant
]).

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
    persistent_term:put({?MODULE, Repo}, with_default(from_config(Config))).

%% The application environment's require_tenant stands for a repo that sets
%% none, as it is when the pool starts.
-spec with_default(kura_repo:settings()) -> kura_repo:settings().
with_default(#{require_tenant := _} = Settings) ->
    Settings;
with_default(Settings) ->
    case kura_settings:read(require_tenant) of
        {ok, true} -> Settings#{require_tenant => true};
        {ok, false} -> Settings#{require_tenant => false};
        _UnsetOrInvalid -> Settings
    end.

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
