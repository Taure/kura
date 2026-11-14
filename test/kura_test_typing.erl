-module(kura_test_typing).
-moduledoc "For tests that give a typed function a term its spec rules out.".

-export([ill_typed/1]).

-spec ill_typed(term()) -> dynamic().
ill_typed(Term) -> Term.
