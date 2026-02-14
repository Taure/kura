-module(kura_multi).

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

-spec new() -> #kura_multi{}.
new() ->
    #kura_multi{}.

-spec insert(#kura_multi{}, atom(), #kura_changeset{} | fun((map()) -> #kura_changeset{})) ->
    #kura_multi{}.
insert(Multi, Name, CSOrFun) ->
    add_operation(Multi, Name, {insert, CSOrFun}).

-spec update(#kura_multi{}, atom(), #kura_changeset{} | fun((map()) -> #kura_changeset{})) ->
    #kura_multi{}.
update(Multi, Name, CSOrFun) ->
    add_operation(Multi, Name, {update, CSOrFun}).

-spec delete(#kura_multi{}, atom(), #kura_changeset{} | fun((map()) -> #kura_changeset{})) ->
    #kura_multi{}.
delete(Multi, Name, CSOrFun) ->
    add_operation(Multi, Name, {delete, CSOrFun}).

-spec run(#kura_multi{}, atom(), fun((map()) -> {ok, term()} | {error, term()})) -> #kura_multi{}.
run(Multi, Name, Fun) ->
    add_operation(Multi, Name, {run, Fun}).

-spec to_list(#kura_multi{}) -> [{atom(), term()}].
to_list(#kura_multi{operations = Ops}) ->
    lists:reverse(Ops).

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
