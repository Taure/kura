-module(kura_tenant).
-moduledoc ~"""
Process dictionary-based multitenancy. Stores the current tenant strategy
(schema prefix or attribute filter) in the process dictionary for use by
Kura repo operations.

Under the attribute strategy every query kura compiles is passed through
`scope_query/2`, which adds the tenant condition to the query and to every
query nested in it. A query whose `from` and joins are all shared schemas
is the exception: every tenant shares its rows, so it gets no condition
(see "Scoped, shared and undeclared schemas" below). Under the prefix
strategy the dialect qualifies each table with the schema when it emits
SQL.

## Scope and lifetime

`with_tenant/2` sets the tenant for one piece of work and puts the process
back as it was when the work ends, however it ends. `put_tenant/1` sets it
for the rest of the process's life. Use `with_tenant/2` in any process that
outlives a request, such as a pool worker or a job runner: a tenant left
there by `put_tenant/1` is the tenant of whatever that process does next.

A process kura starts for a parallel preload takes on the caller's scope
and ends with the preload. No other process inherits a tenant, and kura
keeps none on a pooled connection: a prefix tenant qualifies each table in
the statement and never changes the connection's `search_path`.

## Requiring a tenant

The tenant lives in the process dictionary, so a process that never set
one has none: a spawned process does not inherit its parent's, and a
query run there is not scoped at all. Set `require_tenant` for a repo, in
its config:

```erlang
{kura, [{repos, #{my_repo => #{require_tenant => true, ...}}}]}
```

and a query or a changeset write on that repo with no tenant in the
process raises `{kura, {tenant_required, Source}}` instead of running
unscoped, `Source` being the schema or table it reads or writes. A write
of a changeset is refused before its `prepare_changes` functions and its
`before_*` hooks run. A query is judged by everything it reads (see
`kura_query:sources/1`): what it is from and what it joins, in the query
itself and in every CTE, combination and subquery inside it. Two things
lift the requirement:

- a schema whose `tenant_scoped/0` returns `false`, whatever fields it
  declares;
- a deliberate cross-tenant path, such as an admin report or a
  maintenance job, runs inside `without_tenant/1`.

A repo that does not set it takes the one in the application environment,
so one line covers every repo, and a repo that holds no tenant data, such
as a control plane's, sets `require_tenant => false` for itself:

```erlang
{kura, [{require_tenant, true}]}
```

It is one of the repo's settings (`kura_repo:settings/1`), fixed when the
repo's pool starts; starting the pool again re-reads it. A value that is
not a boolean keeps the pool from starting, with
`{error, {invalid_setting, require_tenant, Value}}`.

Left unset in both places, a missing tenant is not an error, and the first
query or write under a tenant logs one warning per boot, naming the repo.
`false` says the same without the warning. An application that never sets
a tenant sees no warning and has nothing to set. The plan for kura 3.0 is
to require a tenant by default only on a repo that declares a tenancy
strategy; kura 2.x has no such declaration.

Raw SQL through `kura_repo_worker:query/3` is never scoped and never
checked.

## Scoped, shared and undeclared schemas

Under an attribute tenant `{attribute, {Field, Value}}` a schema is in one
of three states. kura decides which in one place, for every read and
every write of the schema.

**Scoped.** The schema declares `Field` as a stored field, one that is not
virtual. It is matched on the field in every query, `update_all` and
`delete_all` and in every `update`, `delete` and upsert of a record, and
its inserts are stamped with the tenant, whatever its `tenant_scoped/0`
returns: a schema that stores the tenant's field holds each tenant's
rows, and under a tenant, saying `false` does not open them to every
tenant; a process without a tenant is not refused for it (see Requiring a
tenant). To read all tenants' rows, run the query in `without_tenant/1`.

**Shared.** The schema exports `tenant_scoped() -> false` and does not
declare `Field`. No tenant owns its rows, so nothing of it names the
tenant. A query, `update_all` or `delete_all` that reads only shared
schemas gets no tenant condition and no warning. An `update`, a `delete`,
an upsert, an `insert`, an `insert_all` and the records written through an
association run as they do in a process with no tenant: no tenant in the
key, no upsert guard, no stamp.

**Undeclared.** The schema does not declare `Field` and has not said it is
shared. On a repo whose `require_tenant` is `true` a statement on it is
refused: a query, `update_all`, `delete_all`, `insert`, `insert_all`,
`update`, `delete` and an upsert raise `{invalid_tenant, Tenant}` before
anything is sent, and a write of a changeset before its `prepare_changes`
functions and its `before_*` hooks run. What sends no statement is not
refused: an `update` with nothing to change, a changeset that is not
valid, and an `insert_all` of no entries. On any other repo it behaves as it did in 2.30, and kura
logs one warning per boot for each such schema, naming the schema and the
field. The tenant condition is added to a query, `update_all`,
`delete_all`, `update`, `delete` and an upsert all the same, so the
statement is scoped when the table has the column and the database
refuses it when it has not. An insert writes the fields its schema
declares, so it writes the row without the tenant. From kura 3.0 an
undeclared schema raises on every repo, so declare the tenant's field in
each schema used under the tenant, or say that the schema is shared.

A table named by a binary, or by an atom that is no schema module, is
matched on the field: kura cannot tell what columns it has, and the
database decides.

kura can tell an undeclared schema only where the query, or a query
combined with it, is from a schema and has no join and no CTE, either of
which can bring the column in. Any other query is left to the database,
without a warning. A write names one schema, so it is always judged.
""".

-include("kura.hrl").

-export([
    put_tenant/1,
    get_tenant/0,
    clear_tenant/0,
    with_tenant/2,
    without_tenant/1,
    current_scope/0,
    adopt/1,
    scope_query/2,
    scope_query/3,
    tenant_for/2,
    write_attribute/2,
    attribute/1,
    scoped_sources/1,
    scoped_in/1,
    permit/3
]).

-define(UNSCOPED, kura_tenant_unscoped).

%% The atoms that head a condition which is not a field's: `{'and', List}`,
%% `{'not', Condition}`, `{exists, Subquery}`, `{fragment, SQL, Params}`.
%% One of them as a tenant's field would scope by that condition, so a new
%% condition head in kura_query must be added here.
-define(IS_CONDITION_HEAD(Atom),
    (Atom =:= 'and' orelse Atom =:= 'or' orelse Atom =:= 'not' orelse Atom =:= exists orelse
        Atom =:= not_exists orelse Atom =:= fragment)
).

-type strategy() :: {prefix, binary()} | {attribute, {atom(), term()}}.

-doc """
What `with_tenant/2` and `without_tenant/1` run: a fun of no arguments, or
`{Module, Function, Args}`, applied as `Module:Function(Args...)`.
""".
-type scoped(Result) :: fun(() -> Result) | {module(), atom(), [term()]}.

-doc "A process's tenant and whether it is inside `without_tenant/1`.".
-opaque scope() :: {undefined | strategy(), boolean()}.

-export_type([strategy/0, scoped/1, scope/0]).

-doc """
Set the current tenant strategy in the process dictionary.

The tenant stays for the life of the process, or until `clear_tenant/0` or
another `put_tenant/1`. That suits a process that serves one request and
ends. A process that outlives the work it was given, such as a pool worker,
a job runner or a `gen_server`, would carry the tenant into the next piece
of work: there, set the tenant with `with_tenant/2`, which ends with the
work.

Raises `{invalid_tenant, Tenant}` on anything that is not a strategy, rather
than storing something that would scope nothing. The attribute value cannot be
`is_nil` or `is_not_nil`, which a condition reads as a test rather than a value,
nor `undefined`, `null` or `nil`: a tenant with no value, such as one read from
a claim that was missing, would count as a tenant, match no row and stamp
inserts with no owner. The attribute field is the name of a field and cannot
be `and`, `or`, `not`, `exists`, `not_exists` or `fragment`: a condition reads
those as its own words, so the tenant would scope by whatever stands in place
of the value. `with_tenant/2` and `adopt/1` refuse the same. `undefined` in
place of the strategy is accepted and leaves the process without a tenant.

Name the field in your own code. A field taken from a request is not a
tenant: kura checks it against the schema only where it can, and refuses a
field the schema does not declare only on a repo that requires a tenant
(see `scope_query/2`).
""".
-spec put_tenant(undefined | strategy()) -> undefined | strategy().
put_tenant(Tenant) ->
    Valid = validate_tenant(Tenant),
    validate_tenant(erlang:put(kura_tenant, Valid)).

-doc "Get the current tenant strategy from the process dictionary.".
-spec get_tenant() -> undefined | strategy().
get_tenant() ->
    validate_tenant(erlang:get(kura_tenant)).

-doc "Remove the current tenant strategy from the process dictionary.".
-spec clear_tenant() -> undefined | strategy().
clear_tenant() ->
    validate_tenant(erlang:erase(kura_tenant)).

-spec validate_tenant(term()) -> undefined | strategy().
validate_tenant(undefined) ->
    undefined;
validate_tenant({prefix, Prefix}) when is_binary(Prefix) ->
    {prefix, Prefix};
validate_tenant({attribute, {Field, Value}}) when
    is_atom(Field),
    not ?IS_CONDITION_HEAD(Field),
    Value =/= is_nil,
    Value =/= is_not_nil,
    Value =/= undefined,
    Value =/= null,
    Value =/= nil
->
    {attribute, {Field, Value}};
validate_tenant(Other) ->
    error({invalid_tenant, Other}).

-doc """
Run `Scoped` under `Tenant` and put the process back as it was, whether
`Scoped` returns, raises, throws or exits: the tenant it had before, or
none. A tenant set inside `Scoped` with `put_tenant/1` ends with it too.

`Scoped` is a fun of no arguments or `{Module, Function, Args}`, which is
applied as it stands and so needs no fun handed across modules. An invalid
`Tenant` raises before anything changes.

```erlang
kura_tenant:with_tenant({prefix, ~"tenant_abc"}, fun() -> my_repo:all(Q) end),
kura_tenant:with_tenant({prefix, ~"tenant_abc"}, {my_repo, all, [Q]}).
```
""".
-spec with_tenant(undefined | strategy(), scoped(Result)) -> Result.
with_tenant(Tenant, Scoped) ->
    Valid = validate_tenant(Tenant),
    Previous = erlang:put(kura_tenant, Valid),
    try
        run(Scoped)
    after
        restore(kura_tenant, Previous)
    end.

-doc """
Run `Scoped` with no tenant, on purpose: queries and writes inside it are
not scoped, and `require_tenant` does not refuse them. The process is put
back as it was afterwards, as with `with_tenant/2`. A `with_tenant/2`
inside `Scoped` scopes as usual.

Use it for the few paths that must cross tenants. It is the only way to
run unscoped once `require_tenant` is set.

`Scoped` is a fun of no arguments or `{Module, Function, Args}`, as for
`with_tenant/2`:

```erlang
kura_tenant:without_tenant(fun() -> my_repo:all(Q) end),
kura_tenant:without_tenant({my_reports, totals, [Month]}).
```
""".
-spec without_tenant(scoped(Result)) -> Result.
without_tenant(Scoped) ->
    Previous = erlang:erase(kura_tenant),
    Unscoped = erlang:put(?UNSCOPED, true),
    try
        run(Scoped)
    after
        restore(?UNSCOPED, Unscoped),
        restore(kura_tenant, Previous)
    end.

-spec run(scoped(Result)) -> Result.
run(Fun) when is_function(Fun, 0) -> Fun();
run({Module, Function, Args}) -> erlang:apply(Module, Function, Args).

%% The value is put back as it was read, valid or not: a scope neither
%% repairs nor adds to what the process held before it.
-spec restore(atom(), term()) -> ok.
restore(Key, undefined) ->
    _ = erlang:erase(Key),
    ok;
restore(Key, Previous) ->
    _ = erlang:put(Key, Previous),
    ok.

-doc """
The calling process's scope: its tenant and whether it is inside
`without_tenant/1`. Hand it to `adopt/1` in a process that does part of
this one's work, as kura's parallel preloads do. A process that is not
handed the scope has no tenant.

```erlang
Scope = kura_tenant:current_scope(),
spawn_link(fun() ->
    ok = kura_tenant:adopt(Scope),
    my_repo:all(kura_query:from(invoice))
end).
```
""".
-spec current_scope() -> scope().
current_scope() ->
    {get_tenant(), erlang:get(?UNSCOPED) =:= true}.

-doc """
Take on a scope read with `current_scope/0` in another process, for the
rest of this process's life. Nothing puts the process back: the tenant
stays, and so does the mark of a scope read inside `without_tenant/1`,
under which `require_tenant` refuses nothing.

So call it only in a process that ends with the work it was started for,
as a spawned preload or a `Task`-like worker does. A process that goes on
to other work, such as a pool worker, a job runner or a `gen_server`,
would do that work under the adopted scope: there, hand the process the
tenant itself (`get_tenant/0`) and run the work in `with_tenant/2`, which
ends with it.

```erlang
Tenant = kura_tenant:get_tenant(),
gen_server:cast(Worker, {run, Tenant, Job}),

handle_cast({run, Tenant, Job}, State) ->
    kura_tenant:with_tenant(Tenant, {my_jobs, run, [Job]}),
    {noreply, State}.
```

Raises `{invalid_tenant, Tenant}` for a scope whose tenant `put_tenant/1`
would refuse.
""".
-spec adopt(scope()) -> ok.
adopt({Tenant, Unscoped}) ->
    restore(kura_tenant, validate_tenant(Tenant)),
    restore(?UNSCOPED, flag(Unscoped)).

-spec flag(boolean()) -> true | undefined.
flag(true) -> true;
flag(false) -> undefined.

%% The tenant to compile `Query` under on `RepoMod`: the process's tenant, or
%% `undefined` when it has none and none is required. Raises
%% {kura, {tenant_required, Source}} as the moduledoc describes.
-doc false.
-spec tenant_for(module(), #kura_query{}) -> undefined | strategy().
tenant_for(RepoMod, Query) ->
    case get_tenant() of
        undefined ->
            ok = unless_required(RepoMod, Query),
            undefined;
        Tenant ->
            ok = warn_unless_decided(RepoMod),
            Tenant
    end.

%% The field and value a write to `Source` on `RepoMod` is matched on and
%% stamped with: the process's attribute tenant, or `none` when it has no
%% such tenant or `Source` is shared. Raises as tenant_for/2 does, and as
%% scope_query/3 does for a schema that does not declare the field.
-doc false.
-spec write_attribute(module(), atom() | binary()) -> none | {atom(), term()}.
write_attribute(RepoMod, Source) ->
    ok = require(RepoMod, Source),
    case get_tenant() of
        {attribute, {Field, _Value}} = Strategy ->
            ok = permit_undeclared(RepoMod, Strategy, Field, undeclared_by(Source, Field));
        _NoAttribute ->
            ok
    end,
    attribute(Source).

%% write_attribute/2 without its checks, for a step of a write or a query
%% that is checked elsewhere.
-doc false.
-spec attribute(atom() | binary()) -> none | {atom(), term()}.
attribute(Source) ->
    case get_tenant() of
        {attribute, {Field, Value}} ->
            case standing(Field, Source) of
                shared -> none;
                _Matched -> {Field, Value}
            end;
        _NoAttribute ->
            none
    end.

-spec require(module(), atom() | binary()) -> ok.
require(RepoMod, Source) ->
    case get_tenant() of
        undefined -> unless_required(RepoMod, Source);
        _Tenant -> warn_unless_decided(RepoMod)
    end.

unless_required(RepoMod, QueryOrSource) ->
    case required(RepoMod) =:= true andalso erlang:get(?UNSCOPED) =/= true of
        true -> check_scoped(QueryOrSource);
        false -> ok
    end.

%% A started repo's settings hold what the application environment said
%% when its pool started, so only a repo kura has not started reads it here.
-spec required(module()) -> boolean() | unset.
required(RepoMod) ->
    case kura_repo_settings:lookup(RepoMod) of
        #{require_tenant := Required} ->
            Required;
        #{} ->
            unset;
        _NotStarted ->
            case kura_repo:settings(RepoMod) of
                #{require_tenant := Required} ->
                    kura_settings:checked_flag(require_tenant, Required);
                _ ->
                    kura_settings:flag(require_tenant)
            end
    end.

-spec warn_unless_decided(module()) -> ok.
warn_unless_decided(RepoMod) ->
    case required(RepoMod) of
        unset ->
            kura_boot_warning:once(require_tenant, #{
                msg =>
                    ~"kura: tenants are in use on a repo whose require_tenant is not set, so a query in a process without a tenant runs unscoped",
                repo => RepoMod,
                fix =>
                    ~"set require_tenant => true in the repo's config, or {kura, [{require_tenant, true}]} for every repo"
            });
        _Decided ->
            ok
    end.

%% A source that names one of the query's CTEs is not a table: the CTE's
%% own body is checked instead. A preload runs as a query of its own and is
%% checked there.
check_scoped(#kura_query{} = Query) ->
    refuse(scoped_sources(Query));
check_scoped(undefined) ->
    ok;
check_scoped(Source) ->
    refuse([Scoped || Scoped <- [Source], tenant_scoped(Scoped)]).

-spec refuse([atom() | binary()]) -> ok.
refuse([]) -> ok;
refuse([Source | _]) -> error({kura, {tenant_required, Source}}).

%% What a query reads that needs a tenant. It depends on the query's
%% sources only, so it is worked out once for a cached shape.
-doc false.
-spec scoped_sources(#kura_query{}) -> [atom() | binary()].
scoped_sources(Query) ->
    scoped_in(kura_query:sources(Query)).

%% scoped_sources/1 for a caller that has the query's sources already.
-doc false.
-spec scoped_in([kura_query:source()]) -> [atom() | binary()].
scoped_in(Sources) ->
    [
        Source
     || {Role, Source} <- Sources,
        Role =:= from orelse Role =:= join,
        tenant_scoped(Source)
    ].

%% tenant_for/2 for a query whose scoped sources are already known.
-doc false.
-spec permit(module(), undefined | strategy(), [atom() | binary()]) -> ok.
permit(_RepoMod, undefined, []) ->
    ok;
permit(RepoMod, undefined, Scoped) ->
    case required(RepoMod) =:= true andalso erlang:get(?UNSCOPED) =/= true of
        true -> refuse(Scoped);
        false -> ok
    end;
permit(RepoMod, _Tenant, _Scoped) ->
    warn_unless_decided(RepoMod).

tenant_scoped(Source) when is_atom(Source) ->
    case kura_schema:is_schema(Source) andalso erlang:function_exported(Source, tenant_scoped, 0) of
        true -> Source:tenant_scoped() =/= false;
        false -> true
    end;
tenant_scoped(_Table) ->
    true.

-doc """
Confine `Query` to the tenant `Strategy` names.

Under `{attribute, {Field, Value}}` the condition `{Field, Value}` is added
to the query, to each CTE and combination, and to every `in` and `exists`
subquery in its `WHERE` and `HAVING` conditions, however deeply nested. The
condition names `Field` unqualified. A top-level query, CTE or union branch over
a table without it fails. An `in` or `exists` subquery over such a table does
not: the database resolves `Field` to the outer query's column, and the inner
table is not scoped. Any other strategy leaves `Query` as it is.

The condition goes through the guard of `kura_query:where/2`, so a `Field`
that is encrypted in a schema of the query raises instead of binding the
tenant value, also when it is deterministic: writes refuse an encrypted
tenant field, so reads do too.

A `Field` that `put_tenant/1` refuses raises `{invalid_tenant, Strategy}`.

A query, or a query nested in `Query`, whose `from` and joins are all
shared schemas gets no condition: no tenant owns its rows. A shared schema
exports `tenant_scoped() -> false` and does not declare `Field` as a
stored field. One other source among them keeps the condition: a schema
that declares `Field`, whatever its `tenant_scoped/0` returns, a schema
without that callback, or a table named by a binary or an atom.

A query from a schema that does not declare `Field` as a stored field
and is not shared is scoped by `Field` all the same: the condition is
added and the database decides, so
the query runs when the table has the column and fails there when it has
not. kura logs one warning for each such schema, naming the schema and the
field, where it can tell: the query, or a query combined with it, is from a
schema and has no join and no CTE, either of which can bring the column in.
A query kura compiles for a repo whose `require_tenant` is `true` raises
`{invalid_tenant, Strategy}` there instead, before anything is sent. kura
3.0 raises for every repo.
""".
-spec scope_query(undefined | strategy(), #kura_query{}) -> #kura_query{}.
scope_query({attribute, {Field, Value}} = Strategy, Query) when is_atom(Field) ->
    _ = validate_tenant(Strategy),
    ok = warn_undeclared(Field, undeclared(Field, Query)),
    scoped(Query, {Field, Value});
scope_query(_Strategy, Query) ->
    Query.

%% scope_query/2 for a query compiled for `RepoMod`, whose require_tenant
%% decides whether a tenant field a schema does not declare is refused.
-doc false.
-spec scope_query(module(), undefined | strategy(), #kura_query{}) -> #kura_query{}.
scope_query(RepoMod, {attribute, {Field, Value}} = Strategy, Query) when is_atom(Field) ->
    _ = validate_tenant(Strategy),
    ok = permit_undeclared(RepoMod, Strategy, Field, undeclared(Field, Query)),
    scoped(Query, {Field, Value});
scope_query(_RepoMod, _Strategy, Query) ->
    Query.

-spec permit_undeclared(module(), strategy(), atom(), [module()]) -> ok.
permit_undeclared(_RepoMod, _Strategy, _Field, []) ->
    ok;
permit_undeclared(RepoMod, Strategy, Field, Schemas) ->
    case required(RepoMod) of
        true -> error({invalid_tenant, Strategy});
        _NotRequired -> warn_undeclared(Field, Schemas)
    end.

-spec warn_undeclared(atom(), [module()]) -> ok.
warn_undeclared(Field, [Schema | Rest]) ->
    ok = kura_boot_warning:once({undeclared_tenant_field, Schema}, #{
        msg =>
            ~"kura: a query or a write under an attribute tenant names a schema that does not declare the tenant's field: the database decides whether a query, an update or a delete runs, and an insert writes the row without the tenant; kura 3.0 refuses them, as require_tenant => true does now",
        schema => Schema,
        field => Field,
        fix =>
            ~"declare the tenant's field in the schema's fields/0, or export tenant_scoped() -> false from a schema every tenant shares"
    }),
    warn_undeclared(Field, Rest);
warn_undeclared(_Field, []) ->
    ok.

%% The schemas `Query` and the queries combined with it are from that do not
%% store `Field`. A join or a CTE can bring the column in, so a query with
%% either is not judged, and neither is what is combined with it.
-spec undeclared(atom(), term()) -> [module()].
undeclared(Field, #kura_query{from = From, joins = [], ctes = [], combinations = Combinations}) ->
    undeclared_by(From, Field) ++ undeclared_in(Field, Combinations);
undeclared(_Field, _Query) ->
    [].

-spec undeclared_in(atom(), term()) -> [module()].
undeclared_in(Field, [{_Kind, Combined} | Rest]) ->
    undeclared(Field, Combined) ++ undeclared_in(Field, Rest);
undeclared_in(_Field, _Rest) ->
    [].

-spec undeclared_by(term(), atom()) -> [module()].
undeclared_by(From, Field) when is_atom(From) ->
    [From || standing(Field, From) =:= undeclared];
undeclared_by(_From, _Field) ->
    [].

%% What a source is under an attribute tenant on `Field`, for every read and
%% every write:
%%
%% - `scoped`: matched on the field and stamped with the tenant. A schema
%%   that stores the field is, whatever its tenant_scoped/0 says, since
%%   its rows are told apart by it. So is a table that is no schema: kura
%%   cannot tell what columns it has, and the database decides.
%% - `shared`: a schema that says `tenant_scoped() -> false` and does not
%%   store the field. No tenant owns its rows, and its table has no column
%%   to name.
%% - `undeclared`: a schema that does not store the field and has not said
%%   it is shared.
-spec standing(atom(), term()) -> scoped | shared | undeclared.
standing(Field, Schema) when is_atom(Schema), Schema =/= undefined ->
    case kura_schema:is_schema(Schema) of
        true -> schema_standing(Field, Schema);
        false -> scoped
    end;
standing(_Field, _Table) ->
    scoped.

-spec schema_standing(atom(), module()) -> scoped | shared | undeclared.
schema_standing(Field, Schema) ->
    case {lists:member(Field, kura_schema:non_virtual_fields(Schema)), tenant_scoped(Schema)} of
        {true, _} -> scoped;
        {false, true} -> undeclared;
        {false, false} -> shared
    end.

-spec scoped(#kura_query{}, {atom(), term()}) -> #kura_query{}.
scoped(Query, {Field, _Value} = Condition) ->
    Nested = Query#kura_query{
        wheres = conditions(Query#kura_query.wheres, Condition),
        havings = conditions(Query#kura_query.havings, Condition),
        ctes = [{Name, scoped(Cte, Condition)} || {Name, Cte} <- Query#kura_query.ctes],
        combinations = [
            {Kind, scoped(Combined, Condition)}
         || {Kind, Combined} <- Query#kura_query.combinations
        ]
    },
    case reads_tenant_rows(Field, Query) of
        true -> kura_query:where_unrewritten(Nested, Condition);
        false -> Nested
    end.

%% A query whose `from` and joins are all shared holds no tenant's rows, and
%% its tables have no tenant column to name. One other table among them
%% keeps the condition, which names that table's column.
-spec reads_tenant_rows(atom(), #kura_query{}) -> boolean().
reads_tenant_rows(Field, #kura_query{from = From, joins = Joins}) ->
    standing(Field, From) =/= shared orelse joins_tenant_rows(Field, Joins).

-spec joins_tenant_rows(atom(), term()) -> boolean().
joins_tenant_rows(_Field, []) ->
    false;
joins_tenant_rows(Field, [{_Type, Table, _On, _As} | Rest]) ->
    standing(Field, Table) =/= shared orelse joins_tenant_rows(Field, Rest);
joins_tenant_rows(_Field, _Joins) ->
    true.

-spec conditions([term()], {atom(), term()}) -> [term()].
conditions(Conditions, Condition) ->
    [scoped_condition(C, Condition) || C <- Conditions].

-spec scoped_condition(term(), {atom(), term()}) -> term().
scoped_condition({Op, Conditions}, Condition) when
    (Op =:= 'and' orelse Op =:= 'or'), is_list(Conditions)
->
    {Op, conditions(Conditions, Condition)};
scoped_condition({'not', Inner}, Condition) ->
    {'not', scoped_condition(Inner, Condition)};
scoped_condition({Field, in, {subquery, #kura_query{} = Sub}}, Condition) ->
    {Field, in, {subquery, scoped(Sub, Condition)}};
scoped_condition({Exists, {subquery, #kura_query{} = Sub}}, Condition) when
    Exists =:= exists; Exists =:= not_exists
->
    {Exists, {subquery, scoped(Sub, Condition)}};
scoped_condition(Other, _Condition) ->
    Other.
