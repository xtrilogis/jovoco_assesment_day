import pandas as pd
import sqlite3

conn = sqlite3.connect("./sales.db")

input_data = [
    "./input_data/customers.csv",
    "./input_data/order_items.csv",
    "./input_data/orders.csv",
    "./input_data/products.csv",
    "./input_data/stores.csv",
]
for file in input_data:
    df = pd.read_csv(file)
    table_name = "bronze_" + file.split("/")[-1].split(".")[0]
    df.to_sql(table_name, conn, if_exists="replace", index=False)

    # Wenn sich die csvs im Szenario ändern, wäre folgender Code besser
    # df.to_sql(table_name, conn, if_exists="append", index=False)

df = pd.read_excel("./input_data/DimDates.xlsx", sheet_name="DimDates")
df.to_sql("bronze_dates", conn, if_exists="replace", index=False)

conn.close()
