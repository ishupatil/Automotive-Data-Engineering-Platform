-- ============================================================
-- Automotive Data Engineering Platform
-- PostgreSQL OLTP Source Tables
-- ============================================================

-- =========================
-- CUSTOMER
-- =========================

CREATE TABLE customer.customers (
    customer_id INTEGER PRIMARY KEY,
    customer_code VARCHAR(20) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    gender VARCHAR(20),
    dob DATE,
    email VARCHAR(255),
    phone VARCHAR(20),
    alternate_phone VARCHAR(20),
    occupation VARCHAR(100),
    annual_income NUMERIC(15,2),
    customer_segment VARCHAR(30),
    address_line1 TEXT,
    city_id INTEGER,
    pan_number VARCHAR(20),
    aadhaar_masked VARCHAR(30),
    registration_date DATE,
    is_active BOOLEAN,
    created_at TIMESTAMP
);


-- =========================
-- DEALER
-- =========================

CREATE TABLE dealer.dealers (
    dealer_id INTEGER PRIMARY KEY,
    dealer_code VARCHAR(20) NOT NULL,
    dealer_name VARCHAR(200),
    dealer_type VARCHAR(50),
    legal_name VARCHAR(200),
    gst_number VARCHAR(30),
    pan_number VARCHAR(20),
    contact_person VARCHAR(100),
    phone VARCHAR(20),
    email VARCHAR(255),
    address_line1 TEXT,
    city_id INTEGER,
    onboarding_date DATE,
    contract_end_date DATE,
    status VARCHAR(30),
    rating NUMERIC(3,2)
);


-- =========================
-- PRODUCT — VEHICLE MODELS
-- =========================

CREATE TABLE product.vehicle_models (
    model_id INTEGER PRIMARY KEY,
    model_code VARCHAR(30) NOT NULL,
    model_name VARCHAR(150),
    category_id INTEGER,
    launch_date DATE,
    end_of_life_date DATE,
    is_active BOOLEAN,
    body_type VARCHAR(50),
    seating_capacity INTEGER
);


-- =========================
-- PRODUCT — VEHICLE VARIANTS
-- =========================

CREATE TABLE product.vehicle_variants (
    variant_id INTEGER PRIMARY KEY,
    variant_code VARCHAR(30) NOT NULL,
    variant_name VARCHAR(150),
    model_id INTEGER,
    fuel_type VARCHAR(30),
    transmission VARCHAR(30),
    engine_cc INTEGER,
    mileage_kmpl NUMERIC(6,2),
    safety_rating NUMERIC(3,1),
    color_options TEXT,
    is_active BOOLEAN
);


-- =========================
-- SALES — ORDERS
-- =========================

CREATE TABLE sales.orders (
    order_id INTEGER PRIMARY KEY,
    order_number VARCHAR(30) NOT NULL,
    booking_id VARCHAR(30),
    customer_id INTEGER,
    dealer_id INTEGER,
    order_date DATE,
    order_status VARCHAR(30),
    sales_executive_id INTEGER,
    discount_id NUMERIC(10,2),
    total_amount NUMERIC(15,2),
    discount_amount NUMERIC(15,2),
    tax_amount NUMERIC(15,2),
    net_amount NUMERIC(15,2),
    payment_mode VARCHAR(30),
    finance_provider VARCHAR(100),
    created_at TIMESTAMP
);


-- =========================
-- SALES — ORDER ITEMS
-- =========================

CREATE TABLE sales.order_items (
    order_item_id INTEGER PRIMARY KEY,
    order_id INTEGER,
    vin VARCHAR(30),
    variant_id INTEGER,
    color VARCHAR(50),
    quantity INTEGER,
    unit_price NUMERIC(15,2),
    discount_amount NUMERIC(15,2),
    tax_amount NUMERIC(15,2),
    line_total NUMERIC(15,2),
    accessories_amount NUMERIC(15,2),
    extended_warranty NUMERIC(15,2)
);


-- =========================
-- SALES — INVOICES
-- =========================

CREATE TABLE sales.invoices (
    invoice_id INTEGER PRIMARY KEY,
    invoice_number VARCHAR(30) NOT NULL,
    order_id INTEGER,
    customer_id INTEGER,
    dealer_id INTEGER,
    invoice_date DATE,
    due_date DATE,
    invoice_amount NUMERIC(15,2),
    tax_amount NUMERIC(15,2),
    cgst_amount NUMERIC(15,2),
    sgst_amount NUMERIC(15,2),
    igst_amount NUMERIC(15,2),
    total_amount NUMERIC(15,2),
    invoice_status VARCHAR(30)
);


-- =========================
-- SALES — PAYMENTS
-- =========================

CREATE TABLE sales.payments (
    payment_id INTEGER PRIMARY KEY,
    payment_reference VARCHAR(50),
    invoice_id INTEGER,
    order_id INTEGER,
    customer_id INTEGER,
    payment_date DATE,
    amount NUMERIC(15,2),
    payment_method VARCHAR(30),
    payment_status VARCHAR(30),
    transaction_id VARCHAR(100),
    bank_name VARCHAR(100)
);