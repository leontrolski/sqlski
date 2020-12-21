from sqlalchemy import Column, Date, ForeignKey, Integer, MetaData, String, Table

metadata = MetaData()
customer = Table(
    "customer",
    metadata,
    Column("customer_id", Integer, primary_key=True),
    Column("username", String, nullable=False),
    Column("postcode", String, nullable=False),
    Column("dob", Date, nullable=False),
)
product = Table(
    "product",
    metadata,
    Column("product_id", Integer, primary_key=True),
    Column("name", String, nullable=False),
    Column("price_cents", Integer, nullable=False),
)
basket = Table(
    "basket",
    metadata,
    Column("basket_id", Integer, primary_key=True),
    Column(
        "customer_id",
        Integer,
        ForeignKey(customer.c.customer_id, ondelete="CASCADE"),
        nullable=False,
    ),
    Column("created_date", Date, nullable=False),
)
purchase = Table(
    "purchase",
    metadata,
    Column("purchase_id", Integer, primary_key=True),
    Column(
        "basket_id",
        Integer,
        ForeignKey(basket.c.basket_id, ondelete="CASCADE"),
        nullable=False,
    ),
    Column(
        "product_id",
        Integer,
        ForeignKey(product.c.product_id, ondelete="CASCADE"),
        nullable=False,
    ),
    Column("qty", Integer, nullable=False),
)
