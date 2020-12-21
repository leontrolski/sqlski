from collections import defaultdict
from dataclasses import dataclass, fields
from typing import Any, Dict, Iterator, List, Optional, Type, Union

from sqlalchemy import Column, MetaData, Table
from sqlalchemy.engine import Connection
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql import Alias, ClauseElement
from sqlalchemy.sql import and_ as sa_and
from sqlalchemy.sql import case, cast
from sqlalchemy.sql import func as sa_func
from sqlalchemy.sql import select as sa_select

from .composite import CompositeArray, CompositeType, register_psycopg2_composite
from .types import (
    BinOperation,
    C,
    Func,
    Nested,
    Operation,
    QueryBundle,
    R,
    RegisterSqlType,
    Relationship,
    Select,
    TypeToSubqueryMap,
    to_is_many_and_type,
)


def make_nested(
    columns: Union[List[Column], Table, Alias],
    label: str,
    many: bool = False,
) -> Nested:
    if isinstance(columns, (Table, Alias)):
        columns = list(columns.c)
    if len(columns) == 0:
        raise RuntimeError("Cannot handle edge case of zero columns")

    name = f"_type_{label}"
    column_types = [Column(col.name, col.type) for col in columns]

    sqlalchemy_type = CompositeType(name, column_types)
    sqlalchemy_array_type = CompositeArray(sqlalchemy_type)

    expression = cast(sa_func.row(*columns), type_=sqlalchemy_type)
    if many:
        expression = sa_func.array_agg(expression, type_=sqlalchemy_array_type)
        expression = case([(sa_func.count(columns[0]) == 0, "{}")], else_=expression)
        expression = expression.label(label)
        expression.type = sqlalchemy_array_type
    else:
        expression = expression.label(label)
        expression.type = sqlalchemy_type

    temp_table = Table(
        name,
        MetaData(),
        *column_types,
        prefixes=["TEMPORARY"],
        postgresql_on_commit="DROP",
    )
    create_type = CreateTable(temp_table)

    def register(conn: Connection) -> ClauseElement:
        conn.execute(create_type)
        register_psycopg2_composite(conn, sqlalchemy_type)
        return expression

    return Nested(
        sqlalchemy_type=sqlalchemy_type,
        expression=expression,
        create_type=create_type,
        register=register,
    )


def _resolve_column(scope: TypeToSubqueryMap, value: Any) -> ClauseElement:
    if not isinstance(value, C):
        return value
    return scope[value.select_type].c[value.name]


def _resolve_operation(scope: TypeToSubqueryMap, operation: Operation) -> str:
    # This should work recursively and it doesn't
    if isinstance(operation, BinOperation):
        left = _resolve_column(scope, operation.left)
        right = _resolve_column(scope, operation.right)
        return getattr(left, operation.attr)(right)
    if isinstance(operation, Func):
        args = [_resolve_column(scope, arg) for arg in operation.args]
        return getattr(sa_func, operation.attr)(*args)
    raise RuntimeError(f"don't yet support operation type: {operation}")


@dataclass
class Mutable:
    grouped_filters: Dict[Type[Select], List[Operation]]
    registers: List[RegisterSqlType]
    scope: TypeToSubqueryMap


def get_select(select_type: Type[R], m: Mutable) -> ClauseElement:
    relationships = select_type.__sqlski_meta__.relationships
    if not relationships:
        query = sa_select(select_type.__sqlski_meta__.selects)
        return query.alias(f"_sub_{select_type.__name__.lower()}")

    extra_selects = []
    group_by = [c.column for c in select_type.__sqlski_meta__.primary_key_columns]
    joined = select_type.__sqlski_meta__.table
    for relationship in select_type.__sqlski_meta__.relationships:
        sub = get_select(relationship.type, m)
        nested = make_nested(sub, label=relationship.name, many=relationship.is_many)
        m.scope[relationship.type] = sub
        m.registers.append(nested.register)
        extra_selects.append(nested.expression)
        join_criteria = _resolve_operation(m.scope, relationship.join)
        filters = m.grouped_filters[relationship.type]
        if filters:
            on = _make_and({relationship.type: sub}, filters)
            join_criteria = sa_and(join_criteria, *on)
        if relationship.is_many:
            joined = joined.outerjoin(sub, join_criteria)
        else:
            joined = joined.join(sub, join_criteria)
            # this is rather unfortunate, as *we* know we don't need it,
            # but the query planner doesn't
            relationship_primary_key_columns = [
                _resolve_column(m.scope, field.default)
                for field in relationship.type.__sqlski_meta__.column_fields
            ]
            group_by.extend(relationship_primary_key_columns)

    selects = [
        _resolve_operation(m.scope, column).label(column.label)
        if isinstance(column, Operation)
        else column
        for column in select_type.__sqlski_meta__.selects
    ]
    query = sa_select(selects + extra_selects).select_from(joined).group_by(*group_by)
    # SQLAlchemy seems unable to preserve custom types
    for orig, new in zip(selects + extra_selects, query.c):
        new.type = orig.type
    return query.alias(f"_sub_{select_type.__name__.lower()}")


def to_select(
    select_type: Type[R], filters: Optional[List[Operation]] = None
) -> QueryBundle:
    m = Mutable(
        grouped_filters=_group_filters(filters or []),
        registers=[],
        scope={
            d: d.__sqlski_meta__.table for d in select_type.__sqlski_meta__.descendants
        },
    )
    sub = get_select(select_type, m)
    operations = m.grouped_filters[select_type]
    if operations:
        query = sa_select([sub])
        query = query.where(sa_and(*_make_and({select_type: sub}, operations)))
    else:
        query = sub.original
    return QueryBundle(query=query, registers=m.registers, scope=m.scope)


def _make_and(
    scope: TypeToSubqueryMap, operations: List[Operation]
) -> List[ClauseElement]:
    return [_resolve_operation(scope, operation) for operation in operations]


def _group_filters(filters: List[Operation]) -> Dict[Type[Select], List[Operation]]:
    grouped_filters: Dict[Type[Select], List[Operation]] = defaultdict(list)
    for operation in filters:
        if isinstance(operation, BinOperation):
            columns = [operation.left, operation.right]
        elif isinstance(operation, Func):
            columns = operation.args
        else:
            raise RuntimeError(f"Unsupported operation type: {operation.__class__}")

        columns = [c for c in columns if isinstance(c, C)]
        result_types = {c.select_type for c in columns}
        if len(result_types) != 1:
            raise RuntimeError(
                f"Require just 1 result type per filter, " f"saw {len(result_types)}"
            )
        grouped_filters[result_types.pop()].append(operation)
    return grouped_filters


def _from_relationship_field(field: Any, row: Any) -> Any:
    is_many, select_type = to_is_many_and_type(field.type)
    if is_many:
        return [from_row(select_type, n) for n in getattr(row, field.name)]
    else:
        return from_row(select_type, getattr(row, field.name))


def from_row(select_type: Type[R], row: Any) -> R:
    d = {}
    for field in fields(select_type):
        if isinstance(field.default, Relationship):
            d[field.name] = _from_relationship_field(field, row)
        else:
            d[field.name] = getattr(row, field.name)
    return select_type(**d)


def do_select(
    conn: Connection, select_type: Type[R], filters: Optional[List[Operation]] = None
) -> Iterator[R]:
    extras = to_select(select_type, filters=filters)
    for register in extras.registers:
        register(conn)
    return (from_row(select_type, row) for row in conn.execute(extras.query))
