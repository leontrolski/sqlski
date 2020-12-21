from dataclasses import dataclass
from typing import Any, Callable, Iterator, List, Union

from sqlalchemy.dialects.postgresql import insert as sa_insert
from sqlalchemy.engine import Connection
from sqlalchemy.sql import ClauseElement, literal, select

from .types import Insert


@dataclass
class Query:
    query: ClauseElement
    _query_function: Callable[[Connection], List[Any]]

    def __call__(self, conn: Connection):
        return self._query_function(conn)


null_select = select([literal(None)]).where(literal(False))


def to_inserts(inserts: Union[List[Insert], Insert]) -> Iterator[Query]:
    if not isinstance(inserts, list):
        inserts = [inserts]

    querys: List[Query] = []

    def add_query(inserts: List[Insert], parent_returnings: List[Any]):
        if not inserts:
            return
        first = inserts[0]
        fields = first.__sqlski_meta__.column_fields
        table = first.__sqlski_meta__.table
        returning = first.__sqlski_meta__.returning_selects
        relationships = first.__sqlski_meta__.relationships
        values = [
            {field.default.column.name: getattr(i, field.name) for field in fields}
            for i in inserts
        ]
        for value, parent_returning in zip(values, parent_returnings):
            value.update(dict(parent_returning))

        query = sa_insert(table).values(values)
        if returning:
            query = query.returning(*returning)

        def f(conn: Connection):
            if not returning:
                if relationships:
                    raise RuntimeError(
                        f"{type(first)} has child inserts, but no RETURNING values"
                    )
                return conn.execute(query)

            returnings = [r for r in conn.execute(query)]
            for relationship in relationships:
                insert_using_column_names = [c.column.name for c in relationship.using]
                parent_returnings = [
                    {name: r[name] for name in insert_using_column_names}
                    for r in returnings
                ]
                all_child_inserts = [getattr(i, relationship.name) for i in inserts]
                all_child_inserts_flat = [
                    (child_insert, returning)
                    for child_inserts, returning in zip(
                        all_child_inserts, parent_returnings
                    )
                    for child_insert in child_inserts
                ]
                if not all_child_inserts_flat:
                    continue
                child_inserts, parent_returnings_flat = zip(*all_child_inserts_flat)
                add_query(child_inserts, parent_returnings_flat)

            return returnings

        querys.append(Query(query, f))

    def iter_querys():
        while querys:
            yield querys.pop()

    add_query(inserts, [{}] * len(inserts))
    return iter_querys()


def do_inserts(conn: Connection, inserts: Union[List[Insert], Insert]) -> List[Any]:
    if not isinstance(inserts, list):
        inserts = [inserts]
    querys = to_inserts(inserts)
    returning = next(querys)(conn)
    for query in querys:
        query(conn)
    return returning
