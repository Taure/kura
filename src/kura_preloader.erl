-module(kura_preloader).

-include("kura.hrl").

-export([
    preload/4,
    do_preload/4
]).

-ifdef(TEST).
-export([
    get_field/2,
    get_field_default/3,
    set_field/3,
    group_by_key/3,
    reverse_maps/2,
    join_bins/2,
    group_m2m_join/5
]).
-endif.

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
    nest_preload(RepoMod, Records1, AssocName, RelatedSchema, Nested, []);
preload_assoc(RepoMod, Records, Schema, AssocName) when is_atom(AssocName) ->
    {ok, Assoc} = kura_schema:association(Schema, AssocName),
    case Assoc#kura_assoc.through of
        [_ | _] = Through ->
            preload_through(RepoMod, Records, Schema, Assoc#kura_assoc.name, Through);
        _ ->
            preload_assoc_direct(RepoMod, Records, Schema, Assoc)
    end.

preload_assoc_direct(RepoMod, Records, _Schema, Assoc = #kura_assoc{type = belongs_to}) ->
    preload_belongs_to(RepoMod, Records, Assoc);
preload_assoc_direct(RepoMod, Records, Schema, Assoc = #kura_assoc{type = has_many}) ->
    preload_has_many(RepoMod, Records, Schema, Assoc);
preload_assoc_direct(RepoMod, Records, Schema, Assoc = #kura_assoc{type = has_one}) ->
    preload_has_one(RepoMod, Records, Schema, Assoc);
preload_assoc_direct(RepoMod, Records, Schema, Assoc = #kura_assoc{type = many_to_many}) ->
    preload_many_to_many(RepoMod, Records, Schema, Assoc).

preload_belongs_to(RepoMod, Records, #kura_assoc{
    name = Name, schema = RelSchema, foreign_key = FK
}) ->
    FKValues = lists:usort([
        get_field(FK, R)
     || R <- Records, get_field_default(FK, R, undefined) =/= undefined
    ]),
    case FKValues of
        [] ->
            [set_field(Name, nil, R) || R <- Records];
        _ ->
            RelPK = kura_schema:primary_key(RelSchema),
            Q = kura_query:where(kura_query:from(RelSchema), {RelPK, in, FKValues}),
            {ok, Related} = kura_repo_worker:all(RepoMod, Q),
            Lookup = to_lookup(Related, RelPK, #{}),
            [
                set_field(
                    Name, get_field_default(get_field_default(FK, R, undefined), Lookup, nil), R
                )
             || R <- Records
            ]
    end.

preload_has_many(RepoMod, Records, Schema, #kura_assoc{
    name = Name, schema = RelSchema, foreign_key = FK
}) ->
    PK = kura_schema:primary_key(Schema),
    PKValues = lists:usort([get_field(PK, R) || R <- Records]),
    Q = kura_query:where(kura_query:from(RelSchema), {FK, in, PKValues}),
    {ok, Related} = kura_repo_worker:all(RepoMod, Q),
    Grouped = group_by_key(Related, FK, #{}),
    [set_field(Name, get_field_default(get_field(PK, R), Grouped, []), R) || R <- Records].

preload_has_one(RepoMod, Records, Schema, #kura_assoc{
    name = Name, schema = RelSchema, foreign_key = FK
}) ->
    PK = kura_schema:primary_key(Schema),
    PKValues = lists:usort([get_field(PK, R) || R <- Records]),
    Q = kura_query:where(kura_query:from(RelSchema), {FK, in, PKValues}),
    {ok, Related} = kura_repo_worker:all(RepoMod, Q),
    Lookup = to_lookup(Related, FK, #{}),
    [set_field(Name, get_field_default(get_field(PK, R), Lookup, nil), R) || R <- Records].

preload_many_to_many(RepoMod, Records, Schema, #kura_assoc{
    name = Name, schema = RelSchema, join_through = JoinThrough, join_keys = {OwnerKey, RelatedKey}
}) ->
    PK = kura_schema:primary_key(Schema),
    PKValues = lists:usort([get_field(PK, R) || R <- Records]),
    case PKValues of
        [] ->
            [set_field(Name, [], R) || R <- Records];
        _ ->
            JoinTableBin = resolve_join_table(JoinThrough),
            OwnerCol = atom_to_binary(OwnerKey, utf8),
            RelatedCol = atom_to_binary(RelatedKey, utf8),
            PlaceholderBins = [
                iolist_to_binary(io_lib:format("$~B", [I]))
             || I <- lists:seq(1, length(PKValues))
            ],
            Placeholders = join_bins(PlaceholderBins, ~", "),
            JoinSQL = iolist_to_binary([
                ~"SELECT ",
                OwnerCol,
                ~", ",
                RelatedCol,
                ~" FROM ",
                JoinTableBin,
                ~" WHERE ",
                OwnerCol,
                ~" IN (",
                Placeholders,
                ~")"
            ]),
            #{rows := JoinRows} = kura_repo_worker:pgo_query(RepoMod, JoinSQL, PKValues),
            RelatedIds = lists:usort([get_field(RelatedKey, JR) || JR <- JoinRows]),
            case RelatedIds of
                [] ->
                    [set_field(Name, [], R) || R <- Records];
                _ ->
                    RelPK = kura_schema:primary_key(RelSchema),
                    Q = kura_query:where(kura_query:from(RelSchema), {RelPK, in, RelatedIds}),
                    {ok, Related} = kura_repo_worker:all(RepoMod, Q),
                    RelLookup = to_lookup(Related, RelPK, #{}),
                    Grouped = group_m2m_join(JoinRows, OwnerKey, RelatedKey, RelLookup, #{}),
                    [
                        set_field(Name, get_field_default(get_field(PK, R), Grouped, []), R)
                     || R <- Records
                    ]
            end
    end.

%%----------------------------------------------------------------------
%% Has many through
%%----------------------------------------------------------------------

preload_through(RepoMod, Records, Schema, Name, Through) ->
    PK = kura_schema:primary_key(Schema),
    %% Preload the full chain on records, then drill down to collect final values
    Preloaded = preload_chain(RepoMod, Records, Schema, Through),
    Grouped = collect_through(Preloaded, PK, Through, #{}),
    [set_field(Name, get_field_default(get_field(PK, R), Grouped, []), R) || R <- Records].

preload_chain(_RepoMod, Records, _Schema, []) ->
    Records;
preload_chain(RepoMod, Records, Schema, [Step | Rest]) ->
    {ok, StepAssoc} = kura_schema:association(Schema, Step),
    StepSchema = StepAssoc#kura_assoc.schema,
    Preloaded = preload_assoc_direct(RepoMod, Records, Schema, StepAssoc),
    case Rest of
        [] ->
            Preloaded;
        _ ->
            Intermediates = extract_assoc_values(Preloaded, Step),
            case Intermediates of
                [] ->
                    Preloaded;
                _ ->
                    SubPreloaded = preload_chain(RepoMod, Intermediates, StepSchema, Rest),
                    SubPK = kura_schema:primary_key(StepSchema),
                    SubLookup = to_lookup(SubPreloaded, SubPK, #{}),
                    reattach_assoc(Preloaded, Step, SubPK, SubLookup)
            end
    end.

extract_assoc_values([], _AssocName) ->
    [];
extract_assoc_values([R | Rest], AssocName) ->
    to_list_val(get_field_default(AssocName, R, [])) ++ extract_assoc_values(Rest, AssocName).

reattach_assoc(Records, AssocName, SubPK, SubLookup) ->
    [
        begin
            OldVals = to_list_val(get_field_default(AssocName, R, [])),
            NewVals = [get_field_default(get_field(SubPK, V), SubLookup, V) || V <- OldVals],
            set_field(AssocName, NewVals, R)
        end
     || R <- Records
    ].

collect_through([], _OwnerPK, _Through, Acc) ->
    Acc;
collect_through([R | Rest], OwnerPK, Through, Acc) ->
    Key = get_field(OwnerPK, R),
    Values = drill_down(R, Through),
    Existing = to_list_val(get_field_default(Key, Acc, [])),
    collect_through(Rest, OwnerPK, Through, Acc#{Key => Existing ++ Values}).

drill_down(Record, []) ->
    [Record];
drill_down(Record, [Step | Rest]) ->
    Vals = to_list_val(get_field_default(Step, Record, [])),
    lists:flatmap(fun(R) -> drill_down(R, Rest) end, Vals).

to_list_val(L) when is_list(L) -> L;
to_list_val(nil) -> [];
to_list_val(undefined) -> [];
to_list_val(M) when is_map(M) -> [M].

resolve_join_table(Table) when is_binary(Table) ->
    Table;
resolve_join_table(Mod) when is_atom(Mod) ->
    Mod:table().

%%----------------------------------------------------------------------
%% Internal: typed map access helpers for eqWAlizer
%%----------------------------------------------------------------------

-spec to_lookup([map()], atom(), map()) -> map().
to_lookup([], _Key, Acc) -> Acc;
to_lookup([Row | Rest], Key, Acc) -> to_lookup(Rest, Key, Acc#{get_field(Key, Row) => Row}).

-spec get_field(atom(), map()) -> term().
get_field(Key, Map) -> maps:get(Key, Map).

-spec get_field_default(term(), map(), term()) -> term().
get_field_default(Key, Map, Default) -> maps:get(Key, Map, Default).

-spec set_field(atom(), term(), map()) -> map().
set_field(Key, Value, Map) -> Map#{Key => Value}.

-spec group_by_key([map()], atom(), map()) -> map().
group_by_key([], _FK, Acc) ->
    Acc;
group_by_key([Rel | Rest], FK, Acc) ->
    Key = get_field(FK, Rel),
    NewAcc = maps:update_with(Key, fun(L) -> L ++ [Rel] end, [Rel], Acc),
    group_by_key(Rest, FK, NewAcc).

-spec group_m2m_join([map()], atom(), atom(), map(), map()) -> map().
group_m2m_join([], _OwnerKey, _RelatedKey, _RelLookup, Acc) ->
    Acc;
group_m2m_join([JR | Rest], OwnerKey, RelatedKey, RelLookup, Acc) ->
    OKey = get_field(OwnerKey, JR),
    RKey = get_field(RelatedKey, JR),
    NewAcc =
        case RelLookup of
            #{RKey := RelRec} ->
                maps:update_with(OKey, fun(L) -> L ++ [RelRec] end, [RelRec], Acc);
            #{} ->
                Acc
        end,
    group_m2m_join(Rest, OwnerKey, RelatedKey, RelLookup, NewAcc).

-spec nest_preload(module(), [map()], atom(), module(), [atom() | {atom(), list()}], [map()]) ->
    [map()].
nest_preload(_RepoMod, [], _AssocName, _RelSchema, _Nested, Acc) ->
    reverse_maps(Acc, []);
nest_preload(RepoMod, [R | Rest], AssocName, RelSchema, Nested, Acc) ->
    Updated =
        case get_field_default(AssocName, R, undefined) of
            undefined ->
                R;
            nil ->
                R;
            Related when is_list(Related) ->
                set_field(AssocName, do_preload(RepoMod, Related, RelSchema, Nested), R);
            Related when is_map(Related) ->
                [U] = do_preload(RepoMod, [Related], RelSchema, Nested),
                set_field(AssocName, U, R)
        end,
    nest_preload(RepoMod, Rest, AssocName, RelSchema, Nested, [Updated | Acc]).

-spec reverse_maps([map()], [map()]) -> [map()].
reverse_maps([], Acc) -> Acc;
reverse_maps([H | T], Acc) -> reverse_maps(T, [H | Acc]).

-spec join_bins([binary()], binary()) -> binary().
join_bins([], _Sep) -> <<>>;
join_bins([H], _Sep) -> H;
join_bins([H | T], Sep) -> <<H/binary, Sep/binary, (join_bins(T, Sep))/binary>>.
