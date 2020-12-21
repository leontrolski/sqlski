# `sqlski`

`sqlski` is an experimental `NRM` - as opposed to an `ORM` - where the `N` stands for "Nested".

## Examples:

First, create SQLAlchemy core [models](tests/data/model.py). Import things from [`sqlski`](sqlski/__init__.py).

### `SELECT`

Describe `SELECT` queries as `dataclass`s:

```python
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
```

Run a query:

```python
customers = do_select(
    conn,
    Customer,
    filters=[
        Customer.upper_cased_username == "HARRY",
        Basket.basket_id == 3,
    ]
)
```

To `yield` plain ol' (almost) instances of said `dataclass`s:

```python
next(customers) == Customer(
    customer_id=3,
    aliased_username="harry",
    upper_cased_username="HARRY",
    baskets=[
        Basket(
            basket_id=3,
            created_date=datetime.date(2017, 1, 7),
            total_price_cents=1690,
            purchases=[
    ...
```

[One `SQL` query was generated](tests/data/expected_query_1.sql) using `postgres`'s nesting capabilities, it looks a bit like:

```sql
SELECT
    _sub_customer.customer_id,
    _sub_customer.aliased_username,
    _sub_customer.upper_cased_username,
    ...
FROM (
    SELECT
        customer.customer_id AS customer_id,
        customer.username AS aliased_username,
        upper(customer.username) AS upper_cased_username,
        array_agg(CAST(row(
            _sub_basket.basket_id,
            _sub_basket.created_date,
            _sub_basket.total_price_cents,
            ...
        ) AS _type_baskets)) END AS baskets
    FROM customer
    LEFT OUTER JOIN (
        SELECT
            basket.basket_id AS basket_id,
            basket.created_date AS created_date,
            sum(_sub_purchase.qty_price_cents) AS total_price_cents,
            ...
        FROM basket
        ...
        ON basket.basket_id = _sub_purchase.basket_id
        GROUP BY basket.basket_id
    ) AS _sub_basket
    ON customer.customer_id = _sub_basket.customer_id AND _sub_basket.basket_id = 3
    GROUP BY customer.customer_id
) AS _sub_customer
WHERE _sub_customer.upper_cased_username = 'HARRY'
```

This SQLAlchemy core query is accessible via `to_select(Customer).query` (as opposed to `do_select(conn, Customer)`.

### `INSERT`

Describe `INSERT` queries as `dataclass`s:

```python
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
```

Run queries like:

```python
customers = [
    Customer(
        username="oliver",
        postcode="SL95GH",
        dob=datetime.date(1990, 1, 20),
        baskets=[
            Basket(
                aliased_created_date=datetime.date(2020, 1, 1),
                purchases=[
    ...
]
customer_ids = [r.customer_id for r in do_inserts(conn, customers)]
```

One `SQL` query per layer of nesting is performed:

```sql
INSERT INTO customer (username, postcode, dob) VALUES
('oliver', 'SL95GH', '1990-01-20'),
('jack', 'N16KL', '1990-02-20')
RETURNING customer.customer_id;

INSERT INTO basket (customer_id, created_date) VALUES
(1, '2020-01-01'),
(2, '2020-01-02'),
(2, '2020-01-03')
RETURNING basket.basket_id;

INSERT INTO purchase (basket_id, product_id, qty) VALUES
(1, 1, 2),
(3, 1, 3);
```

These are accessible via the iterator `to_inserts(products)` - this `yield`s objects with a `.query` that can also be executed by calling with with `(conn)`, a query has to be executed for the next query in the iterator to become available.

### See the [tests](tests) for more examples.

## Why?

It is designed with the following positions in mind:
- Plain ol' serialisable data > fat objects.
- The clever ways ORMs synchronise object state and row state are often confusing. In a not-too-OO codebase, this may be unnecesary.
- Codebases using just raw `SQL`/query builders contain lots of code to munge things between nested objects and flat rows, this _should_ be abstracted away.
- Doing `OUTER JOIN`s to fetch related objects in one query is a bit naff, as we denormalise at query time, only to immediately normalise it again in the application.
- `SELECT`s and `INSERT`s are sufficiently different a lot of the time to warrant their own `dataclass`s.

## Caveats

- Postgres only.
- Not very well tested.
- Missing lots of useful things like `ORDER BY`.
- The `CAST(row(...) AS _type_foo` bits to achieve the nesting db-side is pretty mad, it has to:
  - Create temporary tables for `_type_foo`, these require being in a transaction. (Give me transaction-scoped composite types please postgres!).
  - Register these types with SQLAlchemy via some total wizardy (stolen from [sqlalchemy-utils](https://sqlalchemy-utils.readthedocs.io/en/latest/_modules/sqlalchemy_utils/types/pg_composite.html) ).
- I haven't _really_ thought about `UPDATE`s/`DELETE`s, we can just SQLAlchemy core for that right?
- The typing is a bit off, to get the project working properly with the current interface would require a `mypy` plugin (and is potentially an abuse of the type system full stop).
