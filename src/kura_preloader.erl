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
    group_by_tuple/3,
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
do_preload(RepoMod, Records, Schema, [Assoc]) ->
    preload_assoc(RepoMod, Records, Schema, Assoc);
do_preload(RepoMod, Records, Schema, Assocs) ->
    %% Parallel preload: spawn each association query concurrently,
    %% then merge results sequentially to preserve record ordering.
    Self = self(),
    Refs = lists:map(
        fun(Assoc) ->
            Ref = make_ref(),
            spawn_link(fun() ->
                Result = preload_assoc(RepoMod, Records, Schema, Assoc),
                Self ! {preload_done, Ref, Assoc, Result}
            end),
            {Ref, Assoc}
        end,
        Assocs
    ),
    collect_preload_results(Refs, Records).

collect_preload_results([], Records) ->
    Records;
collect_preload_results([{Ref, Assoc} | Rest], Records) ->
    AssocName =
        case Assoc of
            {Name, _} -> Name;
            Name when is_atom(Name) -> Name
        end,
    receive
        {preload_done, Ref, Assoc, UpdatedRecords} ->
            %% Merge the association field from UpdatedRecords into Records
            Merged = merge_assoc_field(AssocName, Records, UpdatedRecords),
            collect_preload_results(Rest, Merged)
    after 30000 ->
        error({preload_timeout, AssocName})
    end.

preload_assoc(RepoMod, Records, Schema, {AssocName, Opts}) when is_map(Opts) ->
    {ok, Assoc} = kura_schema:association(Schema, AssocName),
    case Assoc#kura_assoc.type of
        has_many -> preload_has_many_cond(RepoMod, Records, Schema, Assoc, Opts);
        has_one -> preload_has_one_cond(RepoMod, Records, Schema, Assoc, Opts);
        belongs_to -> preload_belongs_to(RepoMod, Records, Assoc);
        many_to_many -> preload_many_to_many(RepoMod, Records, Schema, Assoc)
    end;
preload_assoc(RepoMod, Records, Schema, {AssocName, Nested}) when is_list(Nested) ->
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

preload_belongs_to(RepoMod, Records, Assoc = #kura_assoc{name = Name}) ->
    RelSchema = kura_schema:assoc_target(Assoc),
    FKCols = kura_schema:assoc_fields(Assoc),
    TargetKey = assoc_target_key(Assoc, RelSchema),
    FKTuples = lists:usort([fk_tuple(FKCols, R) || R <- Records, fk_complete(FKCols, R)]),
    case FKTuples of
        [] ->
            [set_field(Name, nil, R) || R <- Records];
        _ ->
            Q = kura_query:where(kura_query:from(RelSchema), {TargetKey, in, FKTuples}),
            {ok, Related} = kura_repo_worker:all(RepoMod, Q),
            Lookup = tuple_lookup(Related, TargetKey, #{}),
            [set_field(Name, lookup_by_fk(FKCols, R, Lookup), R) || R <- Records]
    end.

%% The referenced key column(s): an explicit ref target_key, else the
%% given schema's key. belongs_to passes the target schema, has_*/has_one
%% pass the owner schema, matching which side the FK references.
assoc_target_key(Assoc, DefaultSchema) ->
    case kura_schema:assoc_target_key(Assoc) of
        undefined -> kura_schema:key(DefaultSchema);
        Cols -> Cols
    end.

fk_tuple(Cols, Record) ->
    list_to_tuple([get_field(C, Record) || C <- Cols]).

fk_complete(Cols, Record) ->
    lists:all(fun(C) -> get_field_default(C, Record, undefined) =/= undefined end, Cols).

tuple_lookup([], _KeyCols, Acc) ->
    Acc;
tuple_lookup([Rec | Rest], KeyCols, Acc) ->
    tuple_lookup(Rest, KeyCols, Acc#{fk_tuple(KeyCols, Rec) => Rec}).

lookup_by_fk(FKCols, Record, Lookup) ->
    case fk_complete(FKCols, Record) of
        false -> nil;
        true -> get_field_default(fk_tuple(FKCols, Record), Lookup, nil)
    end.

preload_has_many(RepoMod, Records, Schema, Assoc = #kura_assoc{name = Name}) ->
    RelSchema = kura_schema:assoc_target(Assoc),
    FKCols = kura_schema:assoc_fields(Assoc),
    OwnerKey = assoc_target_key(Assoc, Schema),
    OwnerTuples = lists:usort([fk_tuple(OwnerKey, R) || R <- Records]),
    Q = kura_query:where(kura_query:from(RelSchema), {FKCols, in, OwnerTuples}),
    {ok, Related} = kura_repo_worker:all(RepoMod, Q),
    Grouped = group_by_tuple(Related, FKCols, #{}),
    [set_field(Name, get_field_default(fk_tuple(OwnerKey, R), Grouped, []), R) || R <- Records].

preload_has_one(RepoMod, Records, Schema, Assoc = #kura_assoc{name = Name}) ->
    RelSchema = kura_schema:assoc_target(Assoc),
    FKCols = kura_schema:assoc_fields(Assoc),
    OwnerKey = assoc_target_key(Assoc, Schema),
    OwnerTuples = lists:usort([fk_tuple(OwnerKey, R) || R <- Records]),
    Q = kura_query:where(kura_query:from(RelSchema), {FKCols, in, OwnerTuples}),
    {ok, Related} = kura_repo_worker:all(RepoMod, Q),
    Lookup = tuple_lookup(Related, FKCols, #{}),
    [set_field(Name, get_field_default(fk_tuple(OwnerKey, R), Lookup, nil), R) || R <- Records].

preload_has_many_cond(RepoMod, Records, Schema, Assoc = #kura_assoc{name = Name}, Opts) ->
    RelSchema = kura_schema:assoc_target(Assoc),
    FKCols = kura_schema:assoc_fields(Assoc),
    OwnerKey = assoc_target_key(Assoc, Schema),
    OwnerTuples = lists:usort([fk_tuple(OwnerKey, R) || R <- Records]),
    Q0 = kura_query:where(kura_query:from(RelSchema), {FKCols, in, OwnerTuples}),
    Q = apply_preload_opts(Q0, Opts),
    {ok, Related} = kura_repo_worker:all(RepoMod, Q),
    Grouped = group_by_tuple(Related, FKCols, #{}),
    [set_field(Name, get_field_default(fk_tuple(OwnerKey, R), Grouped, []), R) || R <- Records].

preload_has_one_cond(RepoMod, Records, Schema, Assoc = #kura_assoc{name = Name}, Opts) ->
    RelSchema = kura_schema:assoc_target(Assoc),
    FKCols = kura_schema:assoc_fields(Assoc),
    OwnerKey = assoc_target_key(Assoc, Schema),
    OwnerTuples = lists:usort([fk_tuple(OwnerKey, R) || R <- Records]),
    Q0 = kura_query:where(kura_query:from(RelSchema), {FKCols, in, OwnerTuples}),
    Q = apply_preload_opts(Q0, Opts),
    {ok, Related} = kura_repo_worker:all(RepoMod, Q),
    Lookup = tuple_lookup(Related, FKCols, #{}),
    [set_field(Name, get_field_default(fk_tuple(OwnerKey, R), Lookup, nil), R) || R <- Records].

preload_many_to_many(
    RepoMod, Records, Schema, Assoc = #kura_assoc{name = Name, join_through = JoinThrough}
) ->
    RelSchema = kura_schema:assoc_target(Assoc),
    {OwnerCols, RelatedCols} = kura_schema:assoc_join_keys(Assoc),
    OwnerKey = kura_schema:key(Schema),
    OwnerTuples = lists:usort([fk_tuple(OwnerKey, R) || R <- Records]),
    case OwnerTuples of
        [] ->
            [set_field(Name, [], R) || R <- Records];
        _ ->
            JoinTableBin = resolve_join_table(JoinThrough),
            {InSQL, InParams} = composite_in_sql(OwnerCols, OwnerTuples),
            SelectCols = join_bins(
                [quote_ident_bin(atom_to_binary(C, utf8)) || C <- OwnerCols ++ RelatedCols], ~", "
            ),
            JoinSQL = iolist_to_binary([
                ~"SELECT ", SelectCols, ~" FROM ", JoinTableBin, ~" WHERE ", InSQL
            ]),
            #{rows := JoinRows} = kura_repo_worker:pgo_query(RepoMod, JoinSQL, InParams),
            RelTuples = lists:usort([fk_tuple(RelatedCols, JR) || JR <- JoinRows]),
            case RelTuples of
                [] ->
                    [set_field(Name, [], R) || R <- Records];
                _ ->
                    RelKey = kura_schema:key(RelSchema),
                    Q = kura_query:where(kura_query:from(RelSchema), {RelKey, in, RelTuples}),
                    {ok, Related} = kura_repo_worker:all(RepoMod, Q),
                    RelLookup = tuple_lookup(Related, RelKey, #{}),
                    Grouped = group_m2m_join(JoinRows, OwnerCols, RelatedCols, RelLookup, #{}),
                    [
                        set_field(Name, get_field_default(fk_tuple(OwnerKey, R), Grouped, []), R)
                     || R <- Records
                    ]
            end
    end.

-spec composite_in_sql([atom()], [tuple()]) -> {iolist(), [term()]}.
composite_in_sql(Cols, Tuples) ->
    ColList = join_bins([quote_ident_bin(atom_to_binary(C, utf8)) || C <- Cols], ~", "),
    {Groups, Params, _} = m2m_row_groups(Tuples, length(Cols), 1, [], []),
    {[~"(", ColList, ~") IN (", join_bins(Groups, ~", "), ~")"], Params}.

-spec m2m_row_groups([tuple()], non_neg_integer(), pos_integer(), [iolist()], [term()]) ->
    {[iolist()], [term()], pos_integer()}.
m2m_row_groups([], _Arity, Counter, SQLs, Params) ->
    {lists:reverse(SQLs), Params, Counter};
m2m_row_groups([Tuple | Rest], Arity, Counter, SQLs, Params) ->
    {Phs, Next} = m2m_placeholders(Arity, Counter, []),
    Group = [~"(", join_bins(Phs, ~", "), ~")"],
    m2m_row_groups(Rest, Arity, Next, [Group | SQLs], Params ++ tuple_to_list(Tuple)).

-spec m2m_placeholders(non_neg_integer(), pos_integer(), [binary()]) ->
    {[binary()], pos_integer()}.
m2m_placeholders(0, Counter, Acc) ->
    {lists:reverse(Acc), Counter};
m2m_placeholders(N, Counter, Acc) ->
    m2m_placeholders(N - 1, Counter + 1, [
        iolist_to_binary([~"$", integer_to_binary(Counter)]) | Acc
    ]).

quote_ident_bin(Name) when is_binary(Name) ->
    <<$", Name/binary, $">>.

%%----------------------------------------------------------------------
%% Has many through
%%----------------------------------------------------------------------

preload_through(RepoMod, Records, Schema, Name, Through) ->
    PK = kura_schema:primary_key(Schema),
    %% Preload the full chain on records, then drill down to collect final values
    Preloaded = preload_chain(RepoMod, Records, Schema, Through),
    Grouped = collect_through(Preloaded, PK, Through, #{}),
    [set_field(Name, get_field_default(get_field(PK, R), Grouped, []), R) || R <- Records].

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

apply_preload_opts(Q, Opts) ->
    Q1 =
        case Opts of
            #{where := Cond} -> kura_query:where(Q, Cond);
            #{} -> Q
        end,
    Q2 =
        case Opts of
            #{order_by := OrderBy} -> kura_query:order_by(Q1, OrderBy);
            #{} -> Q1
        end,
    case Opts of
        #{limit := Limit} -> kura_query:limit(Q2, Limit);
        #{} -> Q2
    end.

resolve_join_table(Table) when is_binary(Table) ->
    Table;
resolve_join_table(Mod) when is_atom(Mod) ->
    Mod:table().

%%----------------------------------------------------------------------
%% Internal: typed map access helpers for eqWAlizer
%%----------------------------------------------------------------------

-spec merge_assoc_field(atom(), [map()], [map()]) -> [map()].
merge_assoc_field(_AssocName, [], []) ->
    [];
merge_assoc_field(AssocName, [Orig | Origs], [Updated | Upds]) ->
    [
        set_field(AssocName, get_field(AssocName, Updated), Orig)
        | merge_assoc_field(AssocName, Origs, Upds)
    ].

-spec to_lookup([map()], atom(), map()) -> map().
to_lookup([], _Key, Acc) -> Acc;
to_lookup([Row | Rest], Key, Acc) -> to_lookup(Rest, Key, Acc#{get_field(Key, Row) => Row}).

-spec get_field(atom(), map()) -> term().
get_field(Key, Map) -> maps:get(Key, Map).

-spec get_field_default(term(), map(), term()) -> term().
get_field_default(Key, Map, Default) -> maps:get(Key, Map, Default).

-spec set_field(atom(), term(), map()) -> map().
set_field(Key, Value, Map) -> Map#{Key => Value}.

-spec group_by_tuple([map()], [atom()], map()) -> map().
group_by_tuple([], _Cols, Acc) ->
    maps:map(fun(_, V) -> lists:reverse(V) end, Acc);
group_by_tuple([Rel | Rest], Cols, Acc) ->
    Key = fk_tuple(Cols, Rel),
    NewAcc = maps:update_with(Key, fun(L) -> [Rel | L] end, [Rel], Acc),
    group_by_tuple(Rest, Cols, NewAcc).

-spec group_m2m_join([map()], [atom()], [atom()], map(), map()) -> map().
group_m2m_join([], _OwnerCols, _RelatedCols, _RelLookup, Acc) ->
    maps:map(fun(_, V) -> lists:reverse(V) end, Acc);
group_m2m_join([JR | Rest], OwnerCols, RelatedCols, RelLookup, Acc) ->
    OKey = fk_tuple(OwnerCols, JR),
    RKey = fk_tuple(RelatedCols, JR),
    NewAcc =
        case RelLookup of
            #{RKey := RelRec} ->
                maps:update_with(OKey, fun(L) -> [RelRec | L] end, [RelRec], Acc);
            #{} ->
                Acc
        end,
    group_m2m_join(Rest, OwnerCols, RelatedCols, RelLookup, NewAcc).

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
