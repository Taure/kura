-module(kura_preloader).

-include("kura.hrl").

-export([
    preload/4,
    do_preload/4
]).

-spec preload(module(), module(), map() | [map()], [atom() | {atom(), list()}]) ->
    map() | [map()].
preload(RepoMod, Schema, Record, Assocs) when is_map(Record) ->
    [Result] = preload(RepoMod, Schema, [Record], Assocs),
    Result;
preload(_RepoMod, _Schema, [], _Assocs) ->
    [];
preload(RepoMod, Schema, Records, Assocs) when is_list(Records) ->
    do_preload(RepoMod, Records, Schema, Assocs).

do_preload(_RepoMod, Records, _Schema, []) ->
    Records;
do_preload(RepoMod, Records, Schema, [Assoc | Rest]) ->
    Updated = preload_assoc(RepoMod, Records, Schema, Assoc),
    do_preload(RepoMod, Updated, Schema, Rest).

preload_assoc(RepoMod, Records, Schema, {AssocName, Nested}) ->
    Records1 = preload_assoc(RepoMod, Records, Schema, AssocName),
    {ok, Assoc} = kura_schema:association(Schema, AssocName),
    RelatedSchema = Assoc#kura_assoc.schema,
    lists:map(
        fun(R) ->
            case maps:get(AssocName, R, undefined) of
                undefined ->
                    R;
                nil ->
                    R;
                Related when is_list(Related) ->
                    R#{AssocName => do_preload(RepoMod, Related, RelatedSchema, Nested)};
                Related when is_map(Related) ->
                    [Updated] = do_preload(RepoMod, [Related], RelatedSchema, Nested),
                    R#{AssocName => Updated}
            end
        end,
        Records1
    );
preload_assoc(RepoMod, Records, Schema, AssocName) when is_atom(AssocName) ->
    {ok, Assoc} = kura_schema:association(Schema, AssocName),
    case Assoc#kura_assoc.type of
        belongs_to -> preload_belongs_to(RepoMod, Records, Assoc);
        has_many -> preload_has_many(RepoMod, Records, Schema, Assoc);
        has_one -> preload_has_one(RepoMod, Records, Schema, Assoc);
        many_to_many -> preload_many_to_many(RepoMod, Records, Schema, Assoc)
    end.

preload_belongs_to(RepoMod, Records, #kura_assoc{
    name = Name, schema = RelSchema, foreign_key = FK
}) ->
    FKValues = lists:usort([
        maps:get(FK, R)
     || R <- Records, maps:get(FK, R, undefined) =/= undefined
    ]),
    case FKValues of
        [] ->
            [R#{Name => nil} || R <- Records];
        _ ->
            RelPK = RelSchema:primary_key(),
            Q = kura_query:where(kura_query:from(RelSchema), {RelPK, in, FKValues}),
            {ok, Related} = kura_repo_worker:all(RepoMod, Q),
            Lookup = maps:from_list([{maps:get(RelPK, Rel), Rel} || Rel <- Related]),
            [R#{Name => maps:get(maps:get(FK, R, undefined), Lookup, nil)} || R <- Records]
    end.

preload_has_many(RepoMod, Records, Schema, #kura_assoc{
    name = Name, schema = RelSchema, foreign_key = FK
}) ->
    PK = Schema:primary_key(),
    PKValues = lists:usort([maps:get(PK, R) || R <- Records]),
    Q = kura_query:where(kura_query:from(RelSchema), {FK, in, PKValues}),
    {ok, Related} = kura_repo_worker:all(RepoMod, Q),
    Grouped = lists:foldl(
        fun(Rel, Acc) ->
            Key = maps:get(FK, Rel),
            maps:update_with(Key, fun(L) -> L ++ [Rel] end, [Rel], Acc)
        end,
        #{},
        Related
    ),
    [R#{Name => maps:get(maps:get(PK, R), Grouped, [])} || R <- Records].

preload_has_one(RepoMod, Records, Schema, #kura_assoc{
    name = Name, schema = RelSchema, foreign_key = FK
}) ->
    PK = Schema:primary_key(),
    PKValues = lists:usort([maps:get(PK, R) || R <- Records]),
    Q = kura_query:where(kura_query:from(RelSchema), {FK, in, PKValues}),
    {ok, Related} = kura_repo_worker:all(RepoMod, Q),
    Lookup = maps:from_list([{maps:get(FK, Rel), Rel} || Rel <- Related]),
    [R#{Name => maps:get(maps:get(PK, R), Lookup, nil)} || R <- Records].

preload_many_to_many(RepoMod, Records, Schema, #kura_assoc{
    name = Name, schema = RelSchema, join_through = JoinThrough, join_keys = {OwnerKey, RelatedKey}
}) ->
    PK = Schema:primary_key(),
    PKValues = lists:usort([maps:get(PK, R) || R <- Records]),
    case PKValues of
        [] ->
            [R#{Name => []} || R <- Records];
        _ ->
            JoinTable = resolve_join_table(JoinThrough),
            OwnerCol = atom_to_binary(OwnerKey, utf8),
            RelatedCol = atom_to_binary(RelatedKey, utf8),
            Placeholders = lists:join(<<", ">>, [
                iolist_to_binary(io_lib:format("$~B", [I]))
             || I <- lists:seq(1, length(PKValues))
            ]),
            JoinSQL = iolist_to_binary([
                <<"SELECT ">>,
                OwnerCol,
                <<", ">>,
                RelatedCol,
                <<" FROM ">>,
                JoinTable,
                <<" WHERE ">>,
                OwnerCol,
                <<" IN (">>,
                Placeholders,
                <<")">>
            ]),
            #{rows := JoinRows} = kura_repo_worker:pgo_query(RepoMod, JoinSQL, PKValues),
            RelatedIds = lists:usort([maps:get(RelatedKey, JR) || JR <- JoinRows]),
            case RelatedIds of
                [] ->
                    [R#{Name => []} || R <- Records];
                _ ->
                    RelPK = RelSchema:primary_key(),
                    Q = kura_query:where(kura_query:from(RelSchema), {RelPK, in, RelatedIds}),
                    {ok, Related} = kura_repo_worker:all(RepoMod, Q),
                    RelLookup = maps:from_list([{maps:get(RelPK, Rel), Rel} || Rel <- Related]),
                    Grouped = lists:foldl(
                        fun(JR, Acc) ->
                            OKey = maps:get(OwnerKey, JR),
                            RKey = maps:get(RelatedKey, JR),
                            case maps:find(RKey, RelLookup) of
                                {ok, RelRec} ->
                                    maps:update_with(
                                        OKey, fun(L) -> L ++ [RelRec] end, [RelRec], Acc
                                    );
                                error ->
                                    Acc
                            end
                        end,
                        #{},
                        JoinRows
                    ),
                    [R#{Name => maps:get(maps:get(PK, R), Grouped, [])} || R <- Records]
            end
    end.

resolve_join_table(Table) when is_binary(Table) ->
    Table;
resolve_join_table(Mod) when is_atom(Mod) ->
    Mod:table().
