Drop table if exists gold_dim_date;
create table gold_dim_date AS
SELECT
    *
FROM silver_date_dimension;

drop table if exists gold_dim_customers;
create table gold_dim_customers AS
SELECT
    *
FROM silver_customers;

drop table if exists gold_dim_stores;
create table gold_dim_stores AS
SELECT
    *
FROM silver_stores;

drop table if exists gold_dim_products;
create table gold_dim_products AS
SELECT
    *
FROM silver_products;

drop table if exists gold_fact_sales;
CREATE TABLE if not exists gold_fact_sales (
    item_id INT PRIMARY KEY,
    order_id INT NOT NULL,
    customer_id INT NOT NULL,
    product_id INT NOT NULL,
    store_id INT NOT NULL,
    date_id INT NOT NULL,
    order_status VARCHAR(20) NOT NULL,
    quantity DECIMAL(10,2),
    price DECIMAL(10,2),
    revenue DECIMAL(10,2),
    FOREIGN KEY (customer_id) REFERENCES gold_dim_customers(customer_id),
    FOREIGN KEY (store_id) REFERENCES gold_dim_stores(store_id),
    FOREIGN KEY (date_id) REFERENCES gold_dim_date(date_id),
    FOREIGN KEY (product_id) REFERENCES gold_dim_products(product_id)
);

INSERT INTO gold_fact_sales
SELECT
    i.item_id as item_id,
    o.order_id as order_id,
    o.customer_id as customer_id,
    i.product_id as product_id,
    o.store_id as store_id,
    d.date_id as date_id,
    o.status as order_status,
    i.quantity as quantity,
    i.price as price,
    i.quantity * i.price AS revenue
FROM silver_orders o
JOIN silver_order_items i ON o.order_id = i.order_id
JOIN gold_dim_date d ON o.order_date = d.date;
