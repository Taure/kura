-module(m20250101130000_coverage_add_cov_index).
-behaviour(kura_migration).

-include("kura.hrl").

-export([up/0, down/0]).

up() ->
    [{create_index, <<"coverage_items">>, [name], #{unique => true}}].

down() ->
    [{drop_index, <<"coverage_items_name_index">>}].
