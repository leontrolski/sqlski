from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

import sqlparse
from sqlalchemy.dialects.postgresql import psycopg2
from sqlalchemy.sql import ClauseElement


# following class allows in-place pretty printing datetimes etc.
# see https://stackoverflow.com/a/9898141/4865874
class LiteralCompiler(psycopg2.PGCompiler_psycopg2):
    def visit_bindparam(
        self,
        bindparam,
        within_columns_clause=False,
        literal_binds=False,
        **kwargs,
    ):
        return super(LiteralCompiler, self).render_literal_bindparam(
            bindparam,
            within_columns_clause=within_columns_clause,
            literal_binds=literal_binds,
            **kwargs,
        )

    def render_literal_value(self, value, type_):
        if isinstance(value, str):
            value = value.replace("'", "''")
            return f"'{value}'"
        elif isinstance(value, UUID):
            return f"'{value}'"
        elif value is None:
            return "NULL"
        elif isinstance(value, (float, int, Decimal)):
            return repr(value)
        elif isinstance(value, (date, datetime)):
            return f"'{value.isoformat()}'"
        else:
            return "42"
            breakpoint()
            raise NotImplementedError(f"Don't know how to literal-quote value {value}")


def sqlraw(qry: ClauseElement) -> str:
    compiler = LiteralCompiler(psycopg2.dialect(), qry)
    return compiler.process(qry)


def sqlformat(qry: ClauseElement) -> str:
    raw_sql = sqlraw(qry)
    return sqlparse.format(
        raw_sql,
        reindent=True,
        keyword_case="upper",
        indent_width=4,
        indent_tabs=False,
        wrap_after=40,
    )


def sqlprint(qry: ClauseElement) -> None:
    print(sqlformat(qry))
