-module(kura_schema_verify_test_aliased_schema).
-behaviour(kura_schema).

-include_lib("kura/include/kura.hrl").

-export([table/0, fields/0]).

table() -> <<"kura_verify_test_aliased">>.

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true},
        #kura_field{name = email, type = string, column = <<"email_address">>}
    ].
