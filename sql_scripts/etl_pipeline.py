import pandas as pd
import sqlite3

print("Starte ETL Pipeline...")
conn = sqlite3.connect("./sales.db")
print("Verbindung zur Datenbank hergestellt.")

print("Lade Input Daten...")
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

print("Bronze Layer erstellt. Erstelle Silver Layer...")
with open("./sql_scripts/silver.sql") as f:
    conn.executescript(f.read())

print("Silver Layer erstellt. Erstelle Gold Layer...")
with open("./sql_scripts/gold.sql") as f:
    conn.executescript(f.read())

print("ETL Pipeline abgeschlossen.")

conn.close()