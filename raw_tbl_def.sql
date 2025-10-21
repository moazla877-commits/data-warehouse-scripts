create OR REPLACE database SALES_DB;

use database SALES_DB;
create schema if not exists RAW_DATA;
create schema if not exists COMMON_DATA;
create schema if not exists TRANSFORM_DATA;;

use schema COMMON_DATA;

LIST @ETL_S3_STAGE_DATA;

CREATE OR REPLACE STAGE ETL_S3_STAGE_DATA
  URL = 's3://etl-data-csv-s3-bucket/'
  CREDENTIALS = (
    AWS_KEY_ID = 'AKIAQT4FE5LK4T3RI5QV'
    AWS_SECRET_KEY = 'J75pns+UbKJmpr/dUUIa7sPNrBtBR+pZ16RsEtJA'
  )
  --FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1)
  COMMENT = 'S3 csv stage for data loading';


  CREATE OR REPLACE FILE FORMAT COMMON_DATA.csv_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  EMPTY_FIELD_AS_NULL = TRUE
  NULL_IF = ('NULL', 'null')
  COMPRESSION = AUTO
  COMMENT = 'CSV format with comma delimiter, double quotes, and header skip';
  

  use schema RAW_DATA;

  CREATE OR REPLACE TABLE manufacturers (
    manufacturer_id NUMBER PRIMARY KEY,
    manufacturer_name VARCHAR NOT NULL,
    country VARCHAR,
    contact_email VARCHAR,
    phone VARCHAR,
    is_active BOOLEAN DEFAULT TRUE
);

CREATE OR REPLACE TABLE products (
    product_id NUMBER PRIMARY KEY,
    product_name VARCHAR NOT NULL,
    manufacturer_id NUMBER NOT NULL,
    category VARCHAR NOT NULL,
    unit_cost FLOAT NOT NULL,
    unit_price FLOAT NOT NULL,
    sku VARCHAR UNIQUE NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (manufacturer_id) REFERENCES manufacturers(manufacturer_id)
);

CREATE OR REPLACE TABLE customers (
    customer_id NUMBER PRIMARY KEY,
    customer_name VARCHAR NOT NULL,
    type VARCHAR NOT NULL,
    email VARCHAR,
    phone VARCHAR,
    address VARCHAR,
    city VARCHAR,
    state VARCHAR,
    zip_code VARCHAR,
    is_active BOOLEAN DEFAULT TRUE
);

CREATE OR REPLACE TABLE inventory (
    inventory_id NUMBER PRIMARY KEY,
    product_id NUMBER NOT NULL,
    quantity_on_hand NUMBER NOT NULL DEFAULT 0,
    low_stock_threshold NUMBER NOT NULL DEFAULT 10,
    last_restocked_date DATE,
    warehouse_location VARCHAR NOT NULL,
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

CREATE OR REPLACE TABLE sales (
    sale_id NUMBER PRIMARY KEY,
    customer_id NUMBER NOT NULL,
    sale_date DATE NOT NULL,
    total_amount FLOAT NOT NULL,
    status VARCHAR NOT NULL,
    payment_method VARCHAR NOT NULL,
    invoice_number VARCHAR UNIQUE NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE OR REPLACE TABLE sales_line_items (
    sale_item_id NUMBER PRIMARY KEY,
    sale_id NUMBER NOT NULL,
    product_id NUMBER NOT NULL,
    quantity NUMBER NOT NULL,
    unit_price FLOAT NOT NULL,
    line_total_amount FLOAT ,
    FOREIGN KEY (sale_id) REFERENCES sales(sale_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);