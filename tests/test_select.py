import datetime
from dataclasses import asdict
from pathlib import Path

from sqlski import from_row, sqlformat, to_select, do_select, do_inserts

from .data import model
from .data.selects import Basket, Customer, Product, Purchase
from .data.inserts import products, customers
from .helpers import sub


def insert_test_data(conn):
    do_inserts(conn, products)
    do_inserts(conn, customers)


def test_basic_query(conn):
    insert_test_data(conn)
    extras = to_select(Product)
    expected = """
SELECT product.product_id, product.name, product.price_cents
FROM product
    """
    assert sub(sqlformat(extras.query)) == sub(expected)
    actual = [from_row(Product, row) for row in conn.execute(extras.query)]

    expected = [
        Product(product_id=1, name="banana", price_cents=120),
        Product(product_id=2, name="apple", price_cents=90),
        Product(product_id=3, name="ham", price_cents=400),
    ]
    assert actual == expected


def test_basic_filter(conn):
    insert_test_data(conn)
    extras = to_select(Product, filters=[Product.price_cents == 90])
    expected = """
SELECT
    _sub_product.product_id,
    _sub_product.name,
    _sub_product.price_cents
FROM (
    SELECT
        product.product_id AS product_id,
        product.name AS name,
        product.price_cents AS price_cents
    FROM product
) AS _sub_product
WHERE _sub_product.price_cents = 90
    """
    assert sub(sqlformat(extras.query)) == sub(expected)
    actual = [from_row(Product, row) for row in conn.execute(extras.query)]
    expected = [
        Product(product_id=2, name="apple", price_cents=90),
    ]
    assert actual == expected


expected_customers = [
    Customer(
        customer_id=3,
        aliased_username="harry",
        upper_cased_username="HARRY",
        baskets=[
            Basket(
                basket_id=3,
                created_date=datetime.date(2017, 1, 7),
                total_price_cents=1690,
                purchases=[
                    Purchase(
                        qty=4,
                        qty_price_cents=1600,
                        product=Product(product_id=3, name="ham", price_cents=400),
                    ),
                    Purchase(
                        qty=1,
                        qty_price_cents=90,
                        product=Product(product_id=2, name="apple", price_cents=90),
                    ),
                ],
            )
        ],
    ),
]


def test_two_levels_nested(conn):
    insert_test_data(conn)
    extras = to_select(
        Customer,
        filters=[
            Customer.upper_cased_username == "HARRY",
            Basket.basket_id == 3,
        ],
    )
    expected = (Path(__file__).parent / "data/expected_query_1.sql").read_text()
    assert sub(sqlformat(extras.query)) == sub(expected)
    with conn.begin():
        for register in extras.registers:
            register(conn)
        actual = [from_row(Customer, row) for row in conn.execute(extras.query)]

    assert actual == expected_customers

    expected = [
        {
            "aliased_username": "harry",
            "baskets": [
                {
                    "basket_id": 3,
                    "created_date": datetime.date(2017, 1, 7),
                    "purchases": [
                        {
                            "product": {
                                "name": "ham",
                                "price_cents": 400,
                                "product_id": 3,
                            },
                            "qty": 4,
                            "qty_price_cents": 1600,
                        },
                        {
                            "product": {
                                "name": "apple",
                                "price_cents": 90,
                                "product_id": 2,
                            },
                            "qty": 1,
                            "qty_price_cents": 90,
                        },
                    ],
                    "total_price_cents": 1690,
                }
            ],
            "customer_id": 3,
            "upper_cased_username": "HARRY",
        }
    ]
    assert [asdict(n) for n in actual] == expected


def test_two_levels_with_helper(conn):
    insert_test_data(conn)
    with conn.begin():
        actual = do_select(
            conn,
            Customer,
            filters=[
                Customer.upper_cased_username == "HARRY",
                Basket.basket_id == 3,
            ],
        )
    actual = list(actual)
    assert actual == expected_customers
