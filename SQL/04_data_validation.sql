-- ============================================================
-- Automotive Data Engineering Platform
-- PostgreSQL Data Validation
-- ============================================================

-- ============================================================
-- 1. Orders -> Customers
-- ============================================================

SELECT COUNT(*) AS invalid_customer_orders
FROM sales.orders o
LEFT JOIN customer.customers c
    ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;


-- ============================================================
-- 2. Orders -> Dealers
-- ============================================================

SELECT COUNT(*) AS invalid_dealer_orders
FROM sales.orders o
LEFT JOIN dealer.dealers d
    ON o.dealer_id = d.dealer_id
WHERE d.dealer_id IS NULL;


-- ============================================================
-- 3. Order Items -> Orders
-- ============================================================

SELECT COUNT(*) AS invalid_order_items
FROM sales.order_items oi
LEFT JOIN sales.orders o
    ON oi.order_id = o.order_id
WHERE o.order_id IS NULL;


-- ============================================================
-- 4. Order Items -> Vehicle Variants
-- ============================================================

SELECT COUNT(*) AS invalid_variants
FROM sales.order_items oi
LEFT JOIN product.vehicle_variants v
    ON oi.variant_id = v.variant_id
WHERE v.variant_id IS NULL;


-- ============================================================
-- 5. Invoices -> Orders
-- ============================================================

SELECT COUNT(*) AS invalid_invoice_orders
FROM sales.invoices i
LEFT JOIN sales.orders o
    ON i.order_id = o.order_id
WHERE o.order_id IS NULL;


-- ============================================================
-- 6. Payments -> Invoices
-- ============================================================

SELECT COUNT(*) AS invalid_payment_invoices
FROM sales.payments p
LEFT JOIN sales.invoices i
    ON p.invoice_id = i.invoice_id
WHERE i.invoice_id IS NULL;


-- ============================================================
-- 7. Business Rule Validation
-- Payment Order ID must match Invoice Order ID
-- ============================================================

SELECT COUNT(*) AS mismatched_payment_orders
FROM sales.payments p
JOIN sales.invoices i
    ON p.invoice_id = i.invoice_id
WHERE p.order_id <> i.order_id;


Mapping :- 
Orders       → Customers
Orders       → Dealers
Order Items  → Orders
Order Items  → Vehicle Variants
Invoices     → Orders
Payments     → Invoices
Payments     → Invoice Order