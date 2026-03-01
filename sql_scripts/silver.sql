-- TODO: Datenfehler abfangen, z.B. fehlende Werte
-- TODO: formatieren Groß-/Kleinschreibung

-- TODO: proper deduplication statt nur on conflict?
create table if not EXISTS silver_date_dimension (
    date_id int PRIMARY KEY,
    date DATE,
    year INT,
    quarter INT,
    month INT,
    week INT,
    day INT,
    weekday VARCHAR(20)
);


insert or ignore into silver_date_dimension
SELECT
    DateNum AS date_id,
    SUBSTR(DateNum,1,4) || '-' ||
    SUBSTR(DateNum,5,2) || '-' ||
    SUBSTR(DateNum,7,2) AS date,
    SUBSTR(DateNum,1,4) AS year,
    Quarter AS quarter,
    MonthNum AS month,
    WeekNum AS week,
    DayNumOfMonth AS day,
    DayName AS weekday
FROM bronze_dates;

create table if not EXISTS silver_customers (
    customer_id INT PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    city VARCHAR(50) ,
    registration_date DATE,
  type VARCHAR(50),
  FOREIGN KEY (registration_date) REFERENCES silver_date_dimension(date)
);

INSERT INTO silver_customers (
  customer_id,
  name,
  city,
  registration_date,
  type
)
WITH formatted_customers AS (
SELECT
  CAST(CustomerID AS INTEGER) AS customer_id,
  Name AS name,
  City AS city,
  CASE
    WHEN TRIM("Registration Date") GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
      THEN DATE(TRIM("Registration Date"))
    WHEN TRIM("Registration Date") GLOB '[0-9][0-9].[0-9][0-9].[0-9][0-9][0-9][0-9]'
      THEN DATE(
        SUBSTR(TRIM("Registration Date"), 7, 4) || '-' ||
        SUBSTR(TRIM("Registration Date"), 4, 2) || '-' ||
        SUBSTR(TRIM("Registration Date"), 1, 2)
      )
    ELSE NULL
  END AS registration_date,
  "Type" AS type
FROM bronze_customers
)
SELECT
  customer_id,
  name,
  city,
  ( 
      SELECT d.date
      FROM silver_date_dimension d 
      WHERE LOWER(TRIM(d.date)) = LOWER(TRIM(c.registration_date))
      LIMIT 1
    ) AS registration_date,
  type
FROM formatted_customers as c
WHERE 1=1
ON CONFLICT(customer_id) DO UPDATE SET
  name = excluded.name,
  city = excluded.city,
  registration_date = excluded.registration_date,
  type = excluded.type;


create table if not EXISTS silver_stores (
    store_id INT PRIMARY KEY,
    title VARCHAR(150) NOT NULL,
    city VARCHAR(50) NOT NULL,
    region VARCHAR(50) NOT NULL
);

INSERT INTO silver_stores (
  store_id,
  title,
  city,
  region
)
SELECT
  CAST(Store AS INTEGER) AS store_id,
  TRIM(
      UPPER(SUBSTR(Title ,1,1)) ||
      LOWER(SUBSTR(Title,2, INSTR(Title,' ')-1)) || ' ' ||
      UPPER(SUBSTR(Title, INSTR(Title,' ')+1,1)) ||
      LOWER(SUBSTR(Title, INSTR(Title,' ')+2))
    ) AS title,
  City AS city,
  Region AS region
FROM bronze_stores
WHERE 1=1 and title is not null and city is not null and region is not null
ON CONFLICT(store_id) DO UPDATE SET
  title = excluded.title,
  city = excluded.city,
  region = excluded.region;


create table if not EXISTS silver_products (
    product_id INT PRIMARY KEY,
    title VARCHAR(150) NOT NULL,
    category VARCHAR(50) NOT NULL,
    cost DECIMAL(10,2) NOT NULL
);

INSERT INTO silver_products (
  product_id,
  title,
  category,
  cost
)
With products as (
	SELECT
	  CAST(Product AS INTEGER) AS product_id,
	  Title AS title,
	  Category AS category,
	  Cost AS cost
	FROM bronze_products
	Where title is not null and category is not null and cost is not null
)
select 
    * 
from products
WHERE 1=1
ON CONFLICT(product_id) DO UPDATE SET
  title = excluded.title,
  category = excluded.category,
  cost = excluded.cost;


create table if not EXISTS silver_orders (
    order_id INT PRIMARY KEY,
    customer_id INT NOT NULL,
    store_id INT,
    order_date DATE,
    status VARCHAR(20),
    FOREIGN KEY (customer_id) REFERENCES silver_customers(customer_id),
  FOREIGN KEY (store_id) REFERENCES silver_stores(store_id),
  FOREIGN KEY (order_date) REFERENCES silver_date_dimension(date)
);

INSERT INTO silver_orders (
  order_id,
  customer_id,
  store_id,
  order_date,
  status
)
WITH normalized_orders AS (
  SELECT
    CAST("Order" AS INTEGER) AS order_id,
    TRIM("Customer Name") AS customer_name,
    CAST(NULLIF(TRIM("Store"), '') AS INTEGER) AS store_id,
    CASE
      WHEN TRIM("Date") GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
        THEN DATE(TRIM("Date"))
      WHEN TRIM("Date") GLOB '[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9]'
        THEN DATE(
          SUBSTR(TRIM("Date"), 7, 4) || '-' ||
          SUBSTR(TRIM("Date"), 4, 2) || '-' ||
          SUBSTR(TRIM("Date"), 1, 2)
        )
      ELSE NULL
    END AS order_date,
    TRIM("Status") AS status
  FROM bronze_orders
),
matched_orders AS (
  SELECT
    o.order_id,
    COALESCE(
      (
        SELECT c.customer_id
        FROM silver_customers c
        WHERE LOWER(TRIM(c.name)) = LOWER(o.customer_name)
        LIMIT 1
      ),
      (
        SELECT c.customer_id
        FROM silver_customers c
        WHERE LOWER(TRIM(c.name)) = LOWER(
          CASE
            WHEN INSTR(o.customer_name, ' ') > 0
              THEN SUBSTR(o.customer_name, INSTR(o.customer_name, ' ') + 1) || ' ' || SUBSTR(o.customer_name, 1, INSTR(o.customer_name, ' ') - 1)
            ELSE o.customer_name
          END
        )
        LIMIT 1
      )
    ) AS customer_id,
    o.store_id,
    ( 
      SELECT d.date
      FROM silver_date_dimension d 
      WHERE LOWER(TRIM(d.date)) = LOWER(TRIM(o.order_date))
      LIMIT 1
    ) AS order_date,
    o.status
  FROM normalized_orders o
)
SELECT
  order_id,
  customer_id,
  store_id,
  order_date,
  status
FROM matched_orders
WHERE customer_id IS NOT NULL
ON CONFLICT(order_id) DO UPDATE SET
  customer_id = excluded.customer_id,
  store_id = excluded.store_id,
  order_date = excluded.order_date,
  status = excluded.status;

Create table if not EXISTS silver_order_items (
    item_id INT PRIMARY KEY,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity DECIMAL(10,2) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (order_id) REFERENCES silver_orders(order_id),
    FOREIGN KEY (product_id) REFERENCES silver_products(product_id)
);

INSERT INTO silver_order_items (
  item_id,
  order_id,
  product_id,
  quantity,
  price
)
WITH mapped_order_items AS (
  SELECT
    CAST(Item AS INTEGER) AS item_id,
    CAST("Order" AS INTEGER) AS order_id,
    (
      SELECT p.product_id
      FROM silver_products p
      WHERE LOWER(TRIM(p.title)) = LOWER(TRIM(boi."Product"))
      LIMIT 1
    ) AS product_id,
    CAST(Qty AS DECIMAL(10,2)) AS quantity,
    CAST(Price AS DECIMAL(10,2)) AS price
  FROM bronze_order_items boi
)
SELECT
  item_id,
  order_id,
  product_id,
  quantity,
  price
FROM mapped_order_items
WHERE 1=1
  AND item_id IS NOT NULL
  AND order_id IS NOT NULL
  AND product_id IS NOT NULL
  AND quantity IS NOT NULL
  AND price IS NOT NULL
ON CONFLICT(item_id) DO UPDATE SET
  order_id = excluded.order_id,
  product_id = excluded.product_id,
  quantity = excluded.quantity,
  price = excluded.price;
