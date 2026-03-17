-module(kura_tenant_test_schema).
-behaviour(kura_schema).
-include("kura.hrl").

-export([table/0, fields/0]).

table() -> <<"posts">>.

fields() ->
    [
        #kura_field{name = id, type = id, primary_key = true},
        #kura_field{name = title, type = string},
        #kura_field{name = org_id, type = integer}
    ].
