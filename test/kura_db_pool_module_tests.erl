-module(kura_db_pool_module_tests).
-include_lib("eunit/include/eunit.hrl").

%% Verifies that kura_db:get_pool_module/1 reads the configured pool
%% impl from a repo's config and errors when none is set. This is the
%% seam that lets a repo plug in kura_pool_ets (for tests) or any
%% future kura_pool impl without touching kura_db.

-export([
    otp_app/0,
    init/1,
    %% kura_repo behaviour callbacks (unused by these tests but required)
    start/0,
    all/1,
    get/2,
    get_by/2,
    one/1,
    insert/1,
    insert/2,
    update/1,
    delete/1,
    update_all/2,
    delete_all/1,
    insert_all/2,
    insert_all/3,
    exists/1,
    reload/2,
    transaction/1,
    multi/1,
    preload/3,
    query/2
]).

-behaviour(kura_repo).

%%----------------------------------------------------------------------
%% Tests
%%----------------------------------------------------------------------

errors_when_pool_module_unset_test() ->
    persistent_term:put({?MODULE, config}, #{pool => some_pool}),
    try
        ?assertError({no_pool_module_configured, _}, kura_db:get_pool_module(?MODULE))
    after
        persistent_term:erase({?MODULE, config})
    end.

reads_pool_module_from_repo_config_test() ->
    persistent_term:put({?MODULE, config}, #{
        pool => some_pool,
        pool_module => kura_pool_ets
    }),
    try
        ?assertEqual(kura_pool_ets, kura_db:get_pool_module(?MODULE))
    after
        persistent_term:erase({?MODULE, config})
    end.

errors_on_non_atom_pool_module_test() ->
    persistent_term:put({?MODULE, config}, #{
        pool => some_pool,
        pool_module => "not an atom"
    }),
    try
        ?assertError({no_pool_module_configured, _}, kura_db:get_pool_module(?MODULE))
    after
        persistent_term:erase({?MODULE, config})
    end.

%%----------------------------------------------------------------------
%% get_driver_module/1
%%----------------------------------------------------------------------

errors_when_driver_module_unset_test() ->
    persistent_term:put({?MODULE, config}, #{pool => some_pool}),
    try
        ?assertError({no_driver_module_configured, _}, kura_db:get_driver_module(?MODULE))
    after
        persistent_term:erase({?MODULE, config})
    end.

reads_driver_module_from_repo_config_test() ->
    persistent_term:put({?MODULE, config}, #{
        pool => some_pool,
        driver_module => fake_driver_mod
    }),
    try
        ?assertEqual(fake_driver_mod, kura_db:get_driver_module(?MODULE))
    after
        persistent_term:erase({?MODULE, config})
    end.

errors_on_non_atom_driver_module_test() ->
    persistent_term:put({?MODULE, config}, #{
        pool => some_pool,
        driver_module => "not an atom"
    }),
    try
        ?assertError({no_driver_module_configured, _}, kura_db:get_driver_module(?MODULE))
    after
        persistent_term:erase({?MODULE, config})
    end.

%%----------------------------------------------------------------------
%% kura_repo callbacks
%%----------------------------------------------------------------------

otp_app() -> kura.

%% Override init/1 so kura_repo:config/1 returns the persistent_term we set.
init(_DefaultConfig) ->
    case persistent_term:get({?MODULE, config}, undefined) of
        undefined -> #{};
        Config -> Config
    end.

%% Behaviour stubs - never invoked by these tests.
start() -> erlang:error(not_implemented).
all(_) -> erlang:error(not_implemented).
get(_, _) -> erlang:error(not_implemented).
get_by(_, _) -> erlang:error(not_implemented).
one(_) -> erlang:error(not_implemented).
insert(_) -> erlang:error(not_implemented).
insert(_, _) -> erlang:error(not_implemented).
update(_) -> erlang:error(not_implemented).
delete(_) -> erlang:error(not_implemented).
update_all(_, _) -> erlang:error(not_implemented).
delete_all(_) -> erlang:error(not_implemented).
insert_all(_, _) -> erlang:error(not_implemented).
insert_all(_, _, _) -> erlang:error(not_implemented).
exists(_) -> erlang:error(not_implemented).
reload(_, _) -> erlang:error(not_implemented).
transaction(_) -> erlang:error(not_implemented).
multi(_) -> erlang:error(not_implemented).
preload(_, _, _) -> erlang:error(not_implemented).
query(_, _) -> erlang:error(not_implemented).
