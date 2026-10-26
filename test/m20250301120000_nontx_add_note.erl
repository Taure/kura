-module(m20250301120000_nontx_add_note).
-behaviour(kura_migration).

-include("kura.hrl").

-export([up/0, down/0, transaction/0]).

transaction() -> true.

up() ->
    [{alter_table, ~"nontx_items", [{add_column, #kura_column{name = note, type = text}}]}].

down() ->
    [{alter_table, ~"nontx_items", [{drop_column, note}]}].
