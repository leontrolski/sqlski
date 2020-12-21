from datetime import date
from typing import List

from sqlski import C, InsertUsing, Relationship, insert

from .model import basket, customer, product, purchase


@insert
class Product:
    class Returning:
        product_id: int = C(product.c.product_id)

    name: str = C(product.c.name)
    price_cents: int = C(product.c.price_cents)


@insert
class Purchase:
    product_id: int = C(purchase.c.product_id)
    qty: int = C(purchase.c.qty)


@insert
class Basket:
    class Returning:
        basket_id: int = C(basket.c.basket_id)

    aliased_created_date: date = C(basket.c.created_date)
    purchases: List[Purchase] = InsertUsing(Returning.basket_id)


@insert
class Customer:
    class Returning:
        customer_id: int = C(customer.c.customer_id)

    username: str = C(customer.c.username)
    postcode: str = C(customer.c.postcode)
    dob: date = C(customer.c.dob)
    baskets: List[Basket] = InsertUsing(Returning.customer_id)


# test data


products = [
    Product(name="banana", price_cents=120),
    Product(name="apple", price_cents=90),
    Product(name="ham", price_cents=400),
]
customers = [
    Customer(
        username="oliver",
        postcode="SL95GH",
        dob=date(1990, 1, 20),
        baskets=[
            Basket(
                aliased_created_date=date(2017, 1, 3),
                purchases=[
                    Purchase(
                        product_id=1,
                        qty=3,
                    ),
                    Purchase(
                        product_id=3,
                        qty=1,
                    ),
                ],
            ),
            Basket(
                aliased_created_date=date(2017, 1, 4),
                purchases=[
                    Purchase(
                        product_id=2,
                        qty=2,
                    )
                ],
            ),
        ],
    ),
    Customer(username="tom", postcode="NW126GH", dob=date(1957, 11, 3), baskets=[]),
    Customer(
        username="harry",
        postcode="HU2T54",
        dob=date(1983, 5, 20),
        baskets=[
            Basket(
                aliased_created_date=date(2017, 1, 7),
                purchases=[
                    Purchase(
                        product_id=3,
                        qty=4,
                    ),
                    Purchase(
                        product_id=2,
                        qty=1,
                    ),
                ],
            ),
            Basket(aliased_created_date=date(2017, 1, 8), purchases=[]),
        ],
    ),
]
