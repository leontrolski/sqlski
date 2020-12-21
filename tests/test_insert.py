import datetime
import re
from pathlib import Path

from sqlski import from_row, sqlformat, to_inserts, do_inserts

from .data import model
from .data.inserts import customers, products, Customer, Basket, Purchase, Product
from .helpers import sub


# TODO:
# - unnest example
# - no more session - assign to update .append etc


def test_basic_insert(conn):
    customer = Customer(
        username="oliver",
        postcode="SL95GH",
        dob=datetime.date(1990, 1, 20),
        baskets=[],
    )
    querys = to_inserts(customer)
    expected = """
INSERT INTO customer (username, postcode, dob)
VALUES ('oliver', 'SL95GH', '1990-01-20')
RETURNING customer.customer_id
"""
    first = next(querys)
    assert sub(sqlformat(first.query)) == sub(expected)
    assert [r.customer_id for r in first(conn)] == [1]


def test_one_level_nesting(conn):
    customers = [
        Customer(
            username="oliver",
            postcode="SL95GH",
            dob=datetime.date(1990, 1, 20),
            baskets=[
                Basket(
                    aliased_created_date=datetime.date(2020, 1, 1),
                    purchases=[],
                ),
            ],
        ),
        Customer(
            username="jack",
            postcode="N16KL",
            dob=datetime.date(1990, 2, 20),
            baskets=[
                Basket(
                    aliased_created_date=datetime.date(2020, 1, 2),
                    purchases=[],
                ),
                Basket(
                    aliased_created_date=datetime.date(2020, 1, 3),
                    purchases=[],
                ),
            ],
        ),
    ]
    querys = to_inserts(customers)
    expected = """
INSERT INTO customer (username, postcode, dob)
VALUES
('oliver', 'SL95GH', '1990-01-20'),
('jack', 'N16KL', '1990-02-20')
RETURNING customer.customer_id
"""
    first = next(querys)
    assert sub(sqlformat(first.query)) == sub(expected)
    assert [r.customer_id for r in first(conn)] == [1, 2]

    expected = """
INSERT INTO basket (customer_id, created_date)
VALUES
(1, '2020-01-01'),
(2, '2020-01-02'),
(2, '2020-01-03')
RETURNING basket.basket_id
"""
    second = next(querys)
    assert sub(sqlformat(second.query)) == sub(expected)
    assert [r.basket_id for r in second(conn)] == [1, 2, 3]


def test_two_levels_nesting(conn):
    querys = to_inserts(products)
    first = next(querys)
    assert [r.product_id for r in first(conn)] == [1, 2, 3]

    customers = [
        Customer(
            username="oliver",
            postcode="SL95GH",
            dob=datetime.date(1990, 1, 20),
            baskets=[
                Basket(
                    aliased_created_date=datetime.date(2020, 1, 1),
                    purchases=[Purchase(product_id=1, qty=2)],
                ),
            ],
        ),
        Customer(
            username="jack",
            postcode="N16KL",
            dob=datetime.date(1990, 2, 20),
            baskets=[
                Basket(
                    aliased_created_date=datetime.date(2020, 1, 2),
                    purchases=[],
                ),
                Basket(
                    aliased_created_date=datetime.date(2020, 1, 3),
                    purchases=[Purchase(product_id=1, qty=3)],
                ),
            ],
        ),
    ]
    querys = to_inserts(customers)
    expected = """
INSERT INTO customer (username, postcode, dob)
VALUES
('oliver', 'SL95GH', '1990-01-20'),
('jack', 'N16KL', '1990-02-20')
RETURNING customer.customer_id
"""
    first = next(querys)
    assert sub(sqlformat(first.query)) == sub(expected)
    assert [r.customer_id for r in first(conn)] == [1, 2]

    expected = """
INSERT INTO basket (customer_id, created_date)
VALUES
(1, '2020-01-01'),
(2, '2020-01-02'),
(2, '2020-01-03')
RETURNING basket.basket_id
"""
    second = next(querys)
    assert sub(sqlformat(second.query)) == sub(expected)
    assert [r.basket_id for r in second(conn)] == [1, 2, 3]

    third = next(querys)
    expected = """
INSERT INTO purchase (basket_id, product_id, qty)
VALUES
(1, 1, 2),
(3, 1, 3)
"""
    assert sub(sqlformat(third.query)) == sub(expected)
    third(conn)


def test_helpers(conn):
    product_ids = [r.product_id for r in do_inserts(conn, products)]
    assert product_ids == [1, 2, 3]
    customer_ids = [r.customer_id for r in do_inserts(conn, customers)]
    assert customer_ids == [1, 2, 3]
