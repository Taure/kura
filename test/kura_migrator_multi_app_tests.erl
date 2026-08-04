-module(kura_migrator_multi_app_tests).
-moduledoc """
Discovery, ordering and duplicate-guard tests for multi-application migrations.

An asobi extension is a separate OTP application shipping its own
migrations. Before `migration_apps/0` the migrator only ever looked at
the application owning the repo module, so those migrations never ran -
silently. These tests pin the discovery contract without touching a
database: fake applications are `application:load/1`-ed with the module
and dependency lists a real release would have.

The applications are named so that the alphabetical order and the
dependency order disagree (`kura_ma_zbase` is a dependency of
`kura_ma_alpha`), because sorting by name would otherwise pass by
accident.
""".

-include_lib("eunit/include/eunit.hrl").

-define(CORE, kura_ma_core).
-define(EXT, kura_ma_ext).
-define(CLASH, kura_ma_clash).
-define(MID, kura_ma_mid).
-define(LEAF, kura_ma_leaf).
-define(ALPHA, kura_ma_alpha).
-define(ZBASE, kura_ma_zbase).
-define(DUPSELF, kura_ma_dupself).
-define(CYC_X, kura_ma_cyc_x).
-define(CYC_Y, kura_ma_cyc_y).

-define(CORE_A, m20250101100000_ma_core_a).
-define(CORE_C, m20250101300000_ma_core_c).
-define(EXT_B, m20250101200000_ma_ext_b).
-define(CLASH_C, m20250101300000_ma_clash_c).

multi_app_test_() ->
    {setup, fun setup/0, fun cleanup/1, [
        {"a repo without migration_apps/0 sees only its own application",
            fun single_app_repo_sees_only_owning_app/0},
        {"a repo declaring extra applications gets them, owning app included",
            fun declared_apps_include_owning_app/0},
        {"a dependency is ordered before its dependent, not alphabetically",
            fun dependency_orders_before_dependent/0},
        {"an intermediate application still orders the two that ship migrations",
            fun transitive_dependency_is_honoured/0},
        {"an application that is not loaded is named, not skipped",
            fun unloaded_application_is_an_error/0},
        {"a migration_apps/0 that does not return a list of atoms is named",
            fun invalid_migration_apps_is_an_error/0},
        {"a repo module owned by no application discovers nothing",
            fun orphan_repo_module_discovers_nothing/0},
        {"a dependency cycle is reported rather than silently truncated",
            fun application_cycle_is_an_error/0},
        {"single-application discovery is version-ascending, exactly as before",
            fun single_app_discovery_is_unchanged/0},
        {"a dependency's migrations run first even when its versions are newer",
            fun dependency_migrations_run_first/0},
        {"the same version in two applications is a named error",
            fun duplicate_version_across_apps/0},
        {"the same version twice in one application is a named error",
            fun duplicate_version_within_one_app/0},
        {"discovery through the repo surfaces the duplicate", fun duplicate_via_repo/0},
        {"pending filtering preserves the discovered order", fun pending_preserves_order/0}
    ]}.

%%----------------------------------------------------------------------
%% Discovery set
%%----------------------------------------------------------------------

single_app_repo_sees_only_owning_app() ->
    ?assertEqual({ok, [?CORE]}, kura_migrator:migration_apps(kura_single_app_repo)).

declared_apps_include_owning_app() ->
    declare([?EXT]),
    ?assertEqual({ok, [?CORE, ?EXT]}, kura_migrator:migration_apps(kura_multi_app_repo)).

dependency_orders_before_dependent() ->
    %% usort/1 would give [alpha, zbase]; the OTP applications list says
    %% alpha depends on zbase, so zbase must come first.
    ?assertEqual({ok, [?ZBASE, ?ALPHA]}, kura_migrator:topo_sort_apps([?ALPHA, ?ZBASE])).

transitive_dependency_is_honoured() ->
    %% leaf -> mid -> core, and mid ships no migrations of its own.
    ?assertEqual({ok, [?CORE, ?LEAF]}, kura_migrator:topo_sort_apps([?LEAF, ?CORE])).

unloaded_application_is_an_error() ->
    declare([kura_ma_never_loaded]),
    ?assertEqual(
        {error, {migration_apps_not_loaded, [kura_ma_never_loaded]}},
        kura_migrator:migration_apps(kura_multi_app_repo)
    ).

invalid_migration_apps_is_an_error() ->
    declare(not_a_list),
    ?assertEqual(
        {error, {invalid_migration_apps, kura_multi_app_repo, not_a_list}},
        kura_migrator:migration_apps(kura_multi_app_repo)
    ),
    declare([?EXT, "kura_ma_ext"]),
    ?assertEqual(
        {error, {invalid_migration_apps, kura_multi_app_repo, ["kura_ma_ext"]}},
        kura_migrator:migration_apps(kura_multi_app_repo)
    ).

orphan_repo_module_discovers_nothing() ->
    ?assertEqual({ok, []}, kura_migrator:migration_apps(kura_ma_repo_in_no_app)),
    ?assertEqual({ok, []}, kura_migrator:discover_migrations(kura_ma_repo_in_no_app)).

application_cycle_is_an_error() ->
    ?assertEqual(
        {error, {migration_app_cycle, [?CYC_X, ?CYC_Y]}},
        kura_migrator:topo_sort_apps([?CYC_X, ?CYC_Y])
    ).

%%----------------------------------------------------------------------
%% Ordering
%%----------------------------------------------------------------------

single_app_discovery_is_unchanged() ->
    %% The pre-multi-app migrator produced exactly this: every migration
    %% module in the owning application, version ascending.
    ?assertEqual(
        {ok, [{20250101100000, ?CORE_A}, {20250101300000, ?CORE_C}]},
        kura_migrator:discover_migrations(kura_single_app_repo)
    ).

dependency_migrations_run_first() ->
    %% The extension's 20250101200000 sorts between the core's two
    %% versions. Global version ordering would interleave it; the
    %% dependency graph must win, so all of core's run first.
    declare([?EXT]),
    ?assertEqual(
        {ok, [
            {20250101100000, ?CORE_A},
            {20250101300000, ?CORE_C},
            {20250101200000, ?EXT_B}
        ]},
        kura_migrator:discover_migrations(kura_multi_app_repo)
    ).

pending_preserves_order() ->
    Ordered = [{3, m3}, {1, m1}, {2, m2}],
    ?assertEqual(
        [{3, m3}, {2, m2}],
        kura_migrator:pending_migrations(Ordered, [1])
    ),
    ?assertEqual([], kura_migrator:pending_migrations(Ordered, [1, 2, 3])).

%%----------------------------------------------------------------------
%% Duplicate guard
%%----------------------------------------------------------------------

duplicate_version_across_apps() ->
    ?assertEqual(
        {error,
            {duplicate_migration_version, [
                {20250101300000, [{?CORE, ?CORE_C}, {?CLASH, ?CLASH_C}]}
            ]}},
        kura_migrator:collect_migrations([?CORE, ?CLASH])
    ).

duplicate_version_within_one_app() ->
    {error, {duplicate_migration_version, [{Version, Owners}]}} =
        kura_migrator:collect_migrations([?DUPSELF]),
    ?assertEqual(20250401120000, Version),
    ?assertEqual(
        [
            {?DUPSELF, m20250401120000_one},
            {?DUPSELF, m20250401120000_two}
        ],
        lists:sort(Owners)
    ).

duplicate_via_repo() ->
    declare([?CLASH]),
    ?assertMatch(
        {error, {duplicate_migration_version, [{20250101300000, [_, _]}]}},
        kura_migrator:discover_migrations(kura_multi_app_repo)
    ).

%%----------------------------------------------------------------------
%% Fixtures
%%----------------------------------------------------------------------

declare(Apps) ->
    application:set_env(kura, kura_ma_declared_apps, Apps).

setup() ->
    load(?CORE, [kura_multi_app_repo, kura_single_app_repo, ?CORE_A, ?CORE_C], []),
    load(?EXT, [?EXT_B], [?CORE]),
    load(?CLASH, [?CLASH_C], [?CORE]),
    load(?MID, [], [?CORE]),
    load(?LEAF, [], [?MID]),
    load(?ZBASE, [], []),
    load(?ALPHA, [], [?ZBASE]),
    %% Two modules claiming one version inside a single application. The
    %% modules need not exist: discovery reads the application's module
    %% list, and the duplicate is refused before anything is called.
    load(?DUPSELF, [m20250401120000_one, m20250401120000_two], []),
    load(?CYC_X, [], [?CYC_Y]),
    load(?CYC_Y, [], [?CYC_X]),
    declare([]),
    ok.

cleanup(_) ->
    application:unset_env(kura, kura_ma_declared_apps),
    [
        application:unload(A)
     || A <- [?CORE, ?EXT, ?CLASH, ?MID, ?LEAF, ?ZBASE, ?ALPHA, ?DUPSELF, ?CYC_X, ?CYC_Y]
    ],
    ok.

load(Name, Modules, Deps) ->
    Spec =
        {application, Name, [
            {description, "kura multi-app migration discovery test"},
            {vsn, "0.0.1"},
            {modules, Modules},
            {registered, []},
            {applications, [kernel, stdlib | Deps]}
        ]},
    case application:load(Spec) of
        ok -> ok;
        {error, {already_loaded, Name}} -> ok
    end.
