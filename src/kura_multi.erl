-module(kura_multi).
-moduledoc """
Transaction pipelines that group multiple repo operations atomically.

Build a pipeline with `new/0`, add steps with `insert/3`, `update/3`,
`delete/3`, or `run/3`, then execute with `kura_repo_worker:multi/2`.

```erlang
Multi = kura_multi:new(),
M1 = kura_multi:insert(Multi, create_user, UserCS),
M2 = kura_multi:insert(M1, create_profile, fun(#{create_user := User}) ->
    kura_changeset:cast(profile, #{}, #{user_id => maps:get(id, User)}, [user_id])
end),
{ok, Results} = kura_repo_worker:multi(MyRepo, M2).
```
""".

-include("kura.hrl").

-export([
    new/0,
    insert/3,
    update/3,
    delete/3,
    run/3,
    to_list/1,
    append/2
]).

-record(kura_multi, {
    operations = [] :: [{atom(), term()}]
}).

-doc "Create a new empty multi pipeline.".
-spec new() -> #kura_multi{}.
new() ->
    #kura_multi{}.

-doc "Add an insert step. Accepts a changeset or a `fun(PreviousResults) -> Changeset`.".
-spec insert(#kura_multi{}, atom(), #kura_changeset{} | fun((map()) -> #kura_changeset{})) ->
    #kura_multi{}.
insert(Multi, Name, CSOrFun) ->
    add_operation(Multi, Name, {insert, CSOrFun}).

-doc "Add an update step.".
-spec update(#kura_multi{}, atom(), #kura_changeset{} | fun((map()) -> #kura_changeset{})) ->
    #kura_multi{}.
update(Multi, Name, CSOrFun) ->
    add_operation(Multi, Name, {update, CSOrFun}).

-doc "Add a delete step.".
-spec delete(#kura_multi{}, atom(), #kura_changeset{} | fun((map()) -> #kura_changeset{})) ->
    #kura_multi{}.
delete(Multi, Name, CSOrFun) ->
    add_operation(Multi, Name, {delete, CSOrFun}).

-doc "Add an arbitrary function step that receives previous results.".
-spec run(#kura_multi{}, atom(), fun((map()) -> {ok, term()} | {error, term()})) -> #kura_multi{}.
run(Multi, Name, Fun) ->
    add_operation(Multi, Name, {run, Fun}).

-doc "Return the operations list in execution order.".
-spec to_list(#kura_multi{}) -> [{atom(), term()}].
to_list(#kura_multi{operations = Ops}) ->
    lists:reverse(Ops).

-doc "Append the operations of the second multi onto the first.".
-spec append(#kura_multi{}, #kura_multi{}) -> #kura_multi{}.
append(#kura_multi{operations = Ops1}, #kura_multi{operations = Ops2}) ->
    #kura_multi{operations = Ops2 ++ Ops1}.

%%----------------------------------------------------------------------
%% Internal
%%----------------------------------------------------------------------

add_operation(#kura_multi{operations = Ops}, Name, Op) ->
    case lists:keymember(Name, 1, Ops) of
        true -> error({duplicate_step, Name});
        false -> #kura_multi{operations = [{Name, Op} | Ops]}
    end.
