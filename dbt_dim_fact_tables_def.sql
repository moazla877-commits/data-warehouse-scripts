CREATE OR REPLACE TABLE DIM_CUSTOMERS (
    customer_key VARCHAR PRIMARY KEY,
    customer_id NUMBER,
    customer_name VARCHAR,
    type VARCHAR,
    email VARCHAR,
    phone VARCHAR,
    address VARCHAR,
    city VARCHAR,
    state VARCHAR,
    zip_code VARCHAR,
    is_active BOOLEAN,
    valid_from_date DATE,
    valid_to_date DATE,
    is_current BOOLEAN
);

CREATE OR REPLACE TABLE DIM_MANUFACTURERS (
    manufacturer_key VARCHAR PRIMARY KEY,
    manufacturer_id NUMBER,
    manufacturer_name VARCHAR,
    country VARCHAR,
    contact_email VARCHAR,
    phone VARCHAR,
    is_active BOOLEAN,
    valid_from_date DATE,
    valid_to_date DATE,
    is_current BOOLEAN
);

CREATE OR REPLACE TABLE DIM_PRODUCTS (
    product_key VARCHAR PRIMARY KEY,
    product_id NUMBER,
    manufacturer_id NUMBER,
    product_name VARCHAR,
    category VARCHAR,
    unit_cost FLOAT,
    unit_price FLOAT,
    sku VARCHAR,
    is_active BOOLEAN,
    valid_from_date DATE,
    valid_to_date DATE,
    is_current BOOLEAN
);

CREATE OR REPLACE TABLE DIM_INVENTORY (
    inventory_key VARCHAR PRIMARY KEY,
    inventory_id NUMBER,
    product_id NUMBER,
    quantity_on_hand NUMBER,
    low_stock_threshold NUMBER,
    last_restocked_date DATE,
    warehouse_location VARCHAR,
    valid_from_date DATE,
    valid_to_date DATE,
    is_current BOOLEAN
);

CREATE OR REPLACE TABLE FACT_SALES (
    sale_id NUMBER PRIMARY KEY,
    customer_key VARCHAR,
    sale_date DATE,
    total_amount FLOAT,
    status VARCHAR,
    payment_method VARCHAR,
    invoice_number VARCHAR
);

CREATE OR REPLACE TABLE FACT_SALES_LINE_ITEMS (
    sale_item_id NUMBER PRIMARY KEY,
    sale_id NUMBER NOT NULL,
    product_key VARCHAR,
    quantity NUMBER NOT NULL,
    unit_price FLOAT NOT NULL,
    line_total_amount FLOAT NOT NULL,
    FOREIGN KEY (sale_id) REFERENCES FACT_SALES(sale_id),
    FOREIGN KEY (product_key) REFERENCES DIM_PRODUCTS(product_key)
);
