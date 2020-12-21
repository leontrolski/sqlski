# Lovingly stolen from https://sqlalchemy-utils.readthedocs.io/en/latest/_modules/sqlalchemy_utils/types/pg_composite.html
from collections import namedtuple

from psycopg2.extensions import AsIs, adapt, register_adapter, register_type
from psycopg2.extras import CompositeCaster
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import text
from sqlalchemy.sql.expression import FunctionElement
from sqlalchemy.types import SchemaType, TypeDecorator, UserDefinedType, to_instance


class CompositeElement(FunctionElement):
    def __init__(self, base, field, type_):
        self.name = field
        self.type = to_instance(type_)

        super(CompositeElement, self).__init__(base)


@compiles(CompositeElement)
def _compile_pgelem(expr, compiler, **kw):
    return "(%s).%s" % (compiler.process(expr.clauses, **kw), expr.name)


class CompositeArray(ARRAY):
    def _proc_array(self, arr, itemproc, dim, collection):
        if dim is None:
            if isinstance(self.item_type, CompositeType):
                arr = [itemproc(a) for a in arr]
                return arr
        return ARRAY._proc_array(self, arr, itemproc, dim, collection)


class CompositeType(UserDefinedType, SchemaType):
    python_type = tuple

    class comparator_factory(UserDefinedType.Comparator):
        def __getattr__(self, key):
            try:
                type_ = self.type.typemap[key]
            except KeyError:
                raise KeyError(
                    "Type '%s' doesn't have an attribute: '%s'" % (self.name, key)
                )
            return CompositeElement(self.expr, key, type_)

    def __init__(self, name, columns):
        SchemaType.__init__(self)
        self.name = name
        self.columns = columns
        self.type_cls = namedtuple(self.name, [c.name for c in columns])

        class Caster(CompositeCaster):
            def make(obj, values):
                return self.type_cls(*values)

        self.caster = Caster

    def get_col_spec(self):
        return self.name

    def bind_processor(self, dialect):
        def process(value):
            if value is None:
                return None

            processed_value = []
            for i, column in enumerate(self.columns):
                current_value = (
                    value.get(column.name) if isinstance(value, dict) else value[i]
                )

                if isinstance(column.type, TypeDecorator):
                    processed_value.append(
                        column.type.process_bind_param(current_value, dialect)
                    )
                else:
                    processed_value.append(current_value)
            return self.type_cls(*processed_value)

        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is None:
                return None
            cls = value.__class__
            kwargs = {}
            for column in self.columns:
                if isinstance(column.type, TypeDecorator):
                    kwargs[column.name] = column.type.process_result_value(
                        getattr(value, column.name), dialect
                    )
                else:
                    kwargs[column.name] = getattr(value, column.name)
            return cls(**kwargs)

        return process


def from_db(conn, tname):
    qry = text(
        f"""
        SELECT t.oid, typarray, attname, atttypid
        FROM pg_type t
        JOIN pg_namespace ns ON typnamespace = ns.oid
        JOIN pg_attribute a ON attrelid = typrelid
        WHERE typname = :tname
            AND nspname LIKE 'pg_temp%'
            AND attnum > 0
            AND NOT attisdropped
        ORDER BY attnum;
    """
    )
    rows = conn.execute(qry, tname=tname)
    recs = list(rows)

    if not recs:
        raise RuntimeError(
            "PostgreSQL type '%s' not found, have you begun a transaction?" % tname
        )

    type_oid = recs[0][0]
    array_oid = recs[0][1]
    type_attrs = [(r[2], r[3]) for r in recs]

    return CompositeCaster(tname, type_oid, type_attrs, array_oid=array_oid)


def register_psycopg2_composite(conn, composite):
    caster = from_db(conn, composite.name)
    register_type(caster.typecaster, conn.connection.connection)

    if caster.array_typecaster is not None:
        register_type(caster.array_typecaster, conn.connection.connection)

    def adapt_composite(value):
        adapted = [
            adapt(
                getattr(value, column.name)
                if not isinstance(column.type, TypeDecorator)
                else column.type.process_bind_param(
                    getattr(value, column.name), PGDialect_psycopg2()
                )
            )
            for column in composite.columns
        ]
        for value in adapted:
            if hasattr(value, "prepare"):
                value.prepare(conn.connection.connection)
        values = [
            value.getquoted().decode(conn.connection.connection.encoding)
            for value in adapted
        ]
        return AsIs("(%s)::%s" % (", ".join(values), composite.name))

    register_adapter(composite.type_cls, adapt_composite)
