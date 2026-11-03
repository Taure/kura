-module(kura_test_enc_column_name_schema).
-behaviour(kura_schema).

-include("kura.hrl").

-export([table/0, fields/0]).

table() -> ~"enc_column_name".

fields() ->
    Column = column(),
    [
        #kura_field{name = id, type = id, primary_key = true},
        #kura_field{name = ssn, type = {encrypted, string}, column = Column},
        #kura_field{name = plain, type = string, column = Column},
        #kura_field{name = draft, type = {encrypted, string}, virtual = true, column = Column}
    ].

-spec column() -> dynamic().
column() ->
    application:get_env(kura, test_enc_column_name, ~"ssn").
