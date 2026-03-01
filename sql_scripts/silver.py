import sqlite3

conn = sqlite3.connect("sales.db")

with open("./sql_scripts/silver.sql") as f:
    conn.executescript(f.read())


conn.close()