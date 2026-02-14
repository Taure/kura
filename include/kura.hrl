-ifndef(KURA_HRL).
-define(KURA_HRL, true).

-record(kura_field, {
    name :: atom(),
    type :: kura_types:kura_type(),
    column :: binary() | undefined,
    default :: term(),
    primary_key = false :: boolean(),
    nullable = true :: boolean(),
    virtual = false :: boolean()
}).

-record(kura_changeset, {
    valid = true :: boolean(),
    schema :: module() | undefined,
    data = #{} :: map(),
    params = #{} :: map(),
    changes = #{} :: map(),
    errors = [] :: [{atom(), binary()}],
    types = #{} :: #{atom() => kura_types:kura_type()},
    required = [] :: [atom()],
    action :: insert | update | delete | undefined
}).

-record(kura_query, {
    from :: atom() | module() | undefined,
    select = [] :: [atom() | term()],
    wheres = [] :: [term()],
    joins = [] :: [term()],
    order_bys = [] :: [term()],
    group_bys = [] :: [atom()],
    havings = [] :: [term()],
    limit :: non_neg_integer() | undefined,
    offset :: non_neg_integer() | undefined,
    distinct = false :: boolean() | [atom()],
    lock :: binary() | undefined,
    prefix :: binary() | undefined
}).

-record(kura_column, {
    name :: atom(),
    type :: kura_types:kura_type(),
    nullable = true :: boolean(),
    default :: term(),
    primary_key = false :: boolean()
}).

-endif.
