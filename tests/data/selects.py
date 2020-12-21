from datetime import date
from typing import List

from sqlski import C, Relationship, func, select

from .model import basket, customer, product, purchase


@select
class Product:
    product_id: int = C(product.c.product_id)
    name: str = C(product.c.name)
    price_cents: int = C(product.c.price_cents)


@select
class Purchase:
    class Ignore:
        purchase_id: int = C(purchase.c.purchase_id)
        basket_id: int = C(purchase.c.basket_id)
        product_id: int = C(purchase.c.product_id)

    qty: int = C(purchase.c.qty)
    qty_price_cents: int = C(qty * Product.price_cents)
    product: Product = Relationship(Ignore.product_id == Product.product_id)


@select
class Basket:
    class Ignore:
        customer_id: int = C(basket.c.customer_id)

    basket_id: int = C(basket.c.basket_id)
    created_date: date = C(basket.c.created_date)
    total_price_cents: int = C(func.sum(Purchase.qty_price_cents))
    purchases: List[Purchase] = Relationship(basket_id == Purchase.Ignore.basket_id)


@select
class Customer:
    customer_id: int = C(customer.c.customer_id)
    aliased_username: str = C(customer.c.username)
    upper_cased_username: str = C(func.upper(customer.c.username))
    baskets: List[Basket] = Relationship(customer_id == Basket.Ignore.customer_id)
