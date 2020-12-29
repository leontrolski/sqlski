from __future__ import annotations

from dataclasses import Field, dataclass, field, fields
from typing import Callable, Dict, Iterator, List, Optional, Tuple, Type, TypeVar, Union

from sqlalchemy import Column, Table
from sqlalchemy.engine import Connection
from sqlalchemy.sql import ClauseElement

from .composite import CompositeType

RegisterSqlType = Callable[[Connection], ClauseElement]


class Operation:
    label: Optional[str] = None


@dataclass
class BinOperation(Operation):
    left: C
    right: C
    attr: str


@dataclass
class Func(Operation):
    args: List[C]
    attr: str


class Select:
    __sqlski_meta__: ResultMeta = None


class Insert:
    __sqlski_meta__: InsertMeta = None


R = TypeVar("R", Select, Insert)
T = TypeVar("T")


@dataclass
class Nested:
    sqlalchemy_type: CompositeType
    expression: ClauseElement
    register: RegisterSqlType


@dataclass
class RelationshipBundle:
    name: str
    type: Type[Select]
    is_many: bool
    join: Operation
    order_by: Optional[List[Union[Column, Operation]]]


@dataclass
class InsertBundle:
    name: str
    type: Type[Select]
    is_many: bool
    using: List[C]


@dataclass
class ResultMeta:
    table: Table
    column_fields: List[Field]
    primary_key_columns: List[C]
    selects: List[Union[Column, Operation]]
    relationships: List[RelationshipBundle]
    descendants: List[Type[R]] = field(default_factory=list)


@dataclass
class InsertMeta:
    table: Table
    column_fields: List[Field]
    returning_selects: List[Union[Column, Operation]]
    relationships: List[InsertBundle]
    descendants: List[Type[R]] = field(default_factory=list)


@dataclass
class C:
    column: Union[Column, ClauseElement, Operation]
    # these get written by the result decorator
    select_type: Type[Select] = None
    name: str = None

    def __eq__(self, other: C):
        return BinOperation(self, other, "__eq__")

    def __mul__(self, other: C):
        return BinOperation(self, other, "__mul__")


class _FuncMaker:
    def __getattr__(self, attr):
        return lambda *args: Func(args, attr)


func = _FuncMaker()


@dataclass
class Relationship:
    join: Operation
    order_by: Optional[List[Union[Column, Operation]]] = None
    # these get written by the select|insert decorator
    select_type: Type[Select] = None
    name: str = None


@dataclass
class InsertUsing:
    args: List[C]
    # these get written by the select|insert decorator
    select_type: Type[Select] = None
    name: str = None

    def __post_init__(self):
        if not isinstance(self.args, list):
            self.args = [self.args]


TypeToSubqueryMap = Dict[Type[Select], ClauseElement]


@dataclass
class QueryBundle:
    query: ClauseElement
    registers: List[RegisterSqlType]
    scope: TypeToSubqueryMap  # TODO: test the scope is correct


def select(cls) -> Select:
    cls = dataclass(cls)

    def __repr__(self) -> str:
        values = [
            f"{field.name}={repr(getattr(self, field.name))}" for field in fields(self)
        ]
        return f"{cls.__name__}({', '.join(values)})"

    meta = ResultMeta(
        table=_get_table(cls),
        column_fields=list(_yield_column_fields(cls)),
        primary_key_columns=list(_yield_primary_key_columns(cls)),
        selects=list(_yield_selects(cls)),
        relationships=list(_yield_relationships(cls)),
        descendants=list(_yield_descendants(cls)),
    )

    cls.__repr__ = __repr__
    cls.__sqlski_meta__ = meta
    return cls


def insert(cls) -> Insert:
    cls = dataclass(cls)

    meta = InsertMeta(
        table=_get_table(cls),
        column_fields=list(_yield_column_fields(cls)),
        returning_selects=list(_yield_returning_selects(cls)),
        relationships=list(_yield_insert_relationships(cls)),
        descendants=list(_yield_descendants(cls)),
    )

    cls.__sqlski_meta__ = meta
    return cls


def to_is_many_and_type(type_: Union[Type[R], List[Type[R]]]) -> Tuple[bool, R]:
    if not hasattr(type_, "__origin__"):
        return False, type_
    if (type_.__origin__ is not list) or (len(type_.__args__) != 1):
        raise RuntimeError("make sure to type relationship like List[R] or R")
    return True, type_.__args__[0]


def _yield_cls_column_fields(select_type: Type[Select]) -> Iterator[Field]:
    for field in fields(select_type):
        if not isinstance(field.default, (C, Relationship, InsertUsing)):
            raise RuntimeError(
                "fields of a result must have C|Relationship|InsertUsing values as their defaults"
            )
        field.default.select_type = select_type
        field.default.name = field.name
        if isinstance(field.default, C):
            yield field


def _yield_column_fields(select_type: Type[Select]) -> Iterator[Field]:
    for field in _yield_cls_column_fields(select_type):
        yield field
    dont_return_cls = getattr(select_type, "Ignore", type("Ignore", (), {}))
    dont_return_cls = dataclass(dont_return_cls)
    for field in _yield_cls_column_fields(dont_return_cls):
        field.default.select_type = select_type
        yield field


def _yield_relationship_fields(select_type: Type[Select]) -> Iterator[Field]:
    for field in fields(select_type):
        if isinstance(field.default, (Relationship, InsertUsing)):
            yield field


def _yield_relationships(select_type: Type[Select]) -> Iterator[RelationshipBundle]:
    for field in fields(select_type):
        if isinstance(field.default, Relationship):
            is_many, relationship_type = to_is_many_and_type(field.type)
            yield RelationshipBundle(
                name=field.name,
                type=relationship_type,
                is_many=is_many,
                join=field.default.join,
                order_by=field.default.order_by,
            )


def _yield_insert_relationships(select_type: Type[Select]) -> Iterator[InsertBundle]:
    for field in fields(select_type):
        if isinstance(field.default, InsertUsing):
            is_many, relationship_type = to_is_many_and_type(field.type)
            yield InsertBundle(
                name=field.name,
                type=relationship_type,
                is_many=is_many,
                using=field.default.args,
            )


def _yield_selects(select_type: Type[R]) -> Iterator[Union[Column, Operation]]:
    column_fields = _yield_column_fields(select_type)
    for column_field in column_fields:
        column = column_field.default.column
        # we will resolve these later
        if isinstance(column, Operation):
            column.label = column_field.name
            yield column
        elif isinstance(column, Column) and column_field.name == column.name:
            yield column
        else:
            yield column.label(column_field.name)


def _yield_primary_key_columns(select_type: Type[Select]) -> Iterator[C]:
    # TODO: will this work with eg. table1.a * table1.b
    columns = []
    for field in _yield_column_fields(select_type):
        if isinstance(field.default.column, Operation):
            continue
        if field.default.column.primary_key:
            columns.append(field.default)
    if not columns:
        raise RuntimeError("expect at least one primary key column to be selected")

    table = _get_table(select_type)
    if {column for column in table.c if column.primary_key} != {
        c.column for c in columns
    }:
        raise RuntimeError(
            "you must include all the primary keys on the "
            "table so that we can group nested relationships"
        )
    return iter(columns)


def _yield_descendants(select_type: Type[R]) -> Iterator[Type[R]]:
    yield select_type
    for relationship in _yield_relationships(select_type):
        yield from _yield_descendants(relationship.type)


def _get_table(select_type: Type[Select]) -> Table:
    columns = [field.default for field in _yield_column_fields(select_type)]
    referenced_tables = {
        column.column.table for column in columns if isinstance(column.column, Column)
    }
    if len(referenced_tables) != 1:
        raise RuntimeError("can only refer to one table per class")
    return referenced_tables.pop()


def _yield_returning_selects(
    select_type: Type[Select],
) -> Iterator[Union[Column, Operation]]:
    returning_cls = getattr(select_type, "Returning", type("Returning", (), {}))
    returning_cls = dataclass(returning_cls)
    for column in _yield_selects(returning_cls):
        yield column
