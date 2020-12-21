SELECT
    _sub_customer.customer_id,
    _sub_customer.aliased_username,
    _sub_customer.upper_cased_username,
    _sub_customer.baskets
FROM (
    SELECT
        customer.customer_id AS customer_id,
        customer.username AS aliased_username,
        upper(customer.username) AS upper_cased_username,
        CASE WHEN (count(_sub_basket.basket_id) = 0) THEN '{}' ELSE array_agg(CAST(row(
            _sub_basket.basket_id,
            _sub_basket.created_date,
            _sub_basket.total_price_cents,
            _sub_basket.customer_id,
            _sub_basket.purchases
        ) AS _type_baskets)) END AS baskets
    FROM customer
    LEFT OUTER JOIN (
        SELECT
            basket.basket_id AS basket_id,
            basket.created_date AS created_date,
            sum(_sub_purchase.qty_price_cents) AS total_price_cents,
            basket.customer_id AS customer_id,
            CASE WHEN (count(_sub_purchase.qty) = 0) THEN '{}' ELSE array_agg(CAST(row(
                _sub_purchase.qty,
                _sub_purchase.qty_price_cents,
                _sub_purchase.purchase_id,
                _sub_purchase.basket_id,
                _sub_purchase.product_id,
                _sub_purchase.product
            ) AS _type_purchases)) END AS purchases
        FROM basket
        LEFT OUTER JOIN (
            SELECT
                purchase.qty AS qty,
                purchase.qty * _sub_product.price_cents AS qty_price_cents,
                purchase.purchase_id AS purchase_id,
                purchase.basket_id AS basket_id,
                purchase.product_id AS product_id,
                CAST(row(
                    _sub_product.product_id,
                    _sub_product.name,
                    _sub_product.price_cents) AS _type_product
                ) AS product
            FROM purchase
            JOIN (
                SELECT
                    product.product_id AS product_id,
                    product.name AS name,
                    product.price_cents AS price_cents
                FROM product
            ) AS _sub_product
            ON purchase.product_id = _sub_product.product_id
            GROUP BY purchase.purchase_id, _sub_product.product_id, _sub_product.name, _sub_product.price_cents
        ) AS _sub_purchase
        ON basket.basket_id = _sub_purchase.basket_id
        GROUP BY basket.basket_id
    ) AS _sub_basket
    ON customer.customer_id = _sub_basket.customer_id AND _sub_basket.basket_id = 3
    GROUP BY customer.customer_id
) AS _sub_customer
WHERE _sub_customer.upper_cased_username = 'HARRY'
