import pandas as pd
import sqlite3

conn = sqlite3.connect("./sales.db")

df = pd.read_csv("./input_data/customers.csv")
df.to_sql("bronze_customers", conn, if_exists="replace", index=False)

df = pd.read_csv("./input_data/order_items.csv")
df.to_sql("bronze_order_items", conn, if_exists="replace", index=False)

df = pd.read_csv("./input_data/orders.csv")
df.to_sql("bronze_orders", conn, if_exists="replace", index=False)

df = pd.read_csv("./input_data/products.csv")
df.to_sql("bronze_products", conn, if_exists="replace", index=False)

df = pd.read_csv("./input_data/stores.csv")
df.to_sql("bronze_stores", conn, if_exists="replace", index=False)

conn.close()

# TODO go over "if_exists" parameter and decide if "replace" is the right choice here, or if we want to append data instead
# TODO maybe replace this with a sql file