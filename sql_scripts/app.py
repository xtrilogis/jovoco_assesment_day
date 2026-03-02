import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px

st.title("Produkt-Kannibalisierung Analyse")

# Verbindung zur SQLite DB
conn = sqlite3.connect("sales.db")

query = """
SELECT
    p.category,
    p.product_id,
    p.title,
    d.year || '-Q' || d.quarter AS year_quarter,
    SUM(f.revenue) AS revenue
FROM gold_fact_sales f
JOIN gold_dim_products p ON f.product_id = p.product_id
JOIN gold_dim_date d ON f.date_id = d.date_id
GROUP BY p.category, p.product_id, d.year, d.quarter
"""

df = pd.read_sql(query, conn)
conn.close()

# Kategorie Auswahl
category = st.selectbox("Kategorie wählen:", df["category"].unique())

cat_df = df[df["category"] == category]

# Pivot: Quartal x Produkt
pivot = cat_df.pivot_table(
    index="year_quarter",
    columns="title",
    values="revenue",
    fill_value=0
)

if pivot.shape[1] < 2:
    st.warning("Zu wenige Produkte für Korrelationsanalyse.")
else:
    # Korrelationsmatrix
    corr_matrix = pivot.corr()
    # Starke negative Korrelationen anzeigen
    st.subheader("Potenzielle Verdrängung (starke negative Korrelation)")

    pairs = []
    for i in corr_matrix.columns:
        for j in corr_matrix.columns:
            if i < j and corr_matrix.loc[i, j] < -0.5:
                pairs.append((i, j, corr_matrix.loc[i, j]))

    if pairs:
        for p in pairs:
            st.write(f"{p[0]} ↔ {p[1]} | Korrelation: {round(p[2],2)}")
    else:
        st.write("Keine starke negative Korrelation gefunden.")