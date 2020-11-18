import testing.postgresql
from sqlalchemy import create_engine, Table, Column, Date, Integer, String, MetaData, ForeignKey
from sqlalchemy.sql import cast, func, select, literal_column
from sqlalchemy_utils import CompositeArray, CompositeType
from sqlalchemy.dialects.postgresql import ARRAY

metadata = MetaData()
customer = Table("customer", metadata,
    Column("customer_id", Integer, primary_key=True),
    Column("username", String, nullable=False),
    Column("postcode", String, nullable=False),
    Column("dob", Date, nullable=False),
)
product = Table("product", metadata,
    Column("product_id", Integer, primary_key=True),
    Column("name", String, nullable=False),
    Column("price_cents", Integer, nullable=False),
)
basket = Table("basket", metadata,
    Column("basket_id", Integer, primary_key=True),
    Column("customer_id", Integer, ForeignKey(customer.c.customer_id, ondelete="CASCADE"), nullable=False),
    Column("created_date", Date, nullable=False),
)
purchase = Table("purchase", metadata,
    Column("purchase_id", Integer, primary_key=True),
    Column("basket_id", Integer, ForeignKey(basket.c.basket_id, ondelete="CASCADE"), nullable=False),
    Column("product_id", Integer, ForeignKey(product.c.product_id, ondelete="CASCADE"), nullable=False),
    Column("qty", Integer, nullable=False),
)
inserts = [
    customer.insert().values([
        (1, 'oliver', 'SL95GH', '1990-01-20'),
        (2, 'tom', 'NW126GH', '1957-11-03'),
        (3, 'harry', 'HU2T54', '1983-05-11'),
    ]),
    product.insert().values([
        (1, 'banana', 120),
        (2, 'apple', 90),
        (3, 'ham', 400),
    ]),
    basket.insert().values([
        (1, 1, '2017-01-03'),
        (2, 1, '2017-01-04'),
        (3, 3, '2017-01-07'),
        (4, 3, '2017-01-08'),
    ]),
    purchase.insert().values([
        (1, 1, 1, 3),
        (2, 1, 3, 1),
        (3, 2, 2, 2),
        (4, 3, 3, 4),
    ]),
]


_basket = CompositeType(
    '_basket',
    [
        basket.c.basket_id,
        basket.c.customer_id,
        basket.c.created_date,
    ]
)
foo = CompositeArray(_basket)

many_baskets = func.array_agg(cast(func.row(
    basket.c.basket_id,
    basket.c.customer_id,
    basket.c.created_date,
), type_=_basket), type_=foo).label("baskets")

with testing.postgresql.Postgresql() as postgresql:
    engine = create_engine(postgresql.url())
    metadata.create_all(engine)
    conn = engine.connect()

    for insert in inserts:
        conn.execute(insert)

    qry = (
        select([
            customer,
            many_baskets,
        ])
        .select_from(
            customer
            .join(basket, customer.c.customer_id == basket.c.basket_id)
        )
        .group_by(customer.c.customer_id)
    )
    print(qry)
    rows = conn.execute(qry)
    for row in rows:
        print(row.username)
        for basket in row.baskets:
            print(basket)
        # breakpoint()
        # print(row)
    conn.close()


# select created_date(basket)
# from (select unnest(baskets) as basket
# from (
#   select baskets from (
#       select
#           customer.*,
#           array_agg(cast(row(basket.created_date) as _basket)) as baskets
#       from customer
#       join basket
#       using (customer_id)
#       group by customer_id
#   ) as _ limit 1)
# as __) as ___;
