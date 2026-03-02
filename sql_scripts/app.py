import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
# Verbindung zur SQLite DB
conn = sqlite3.connect("sales.db")


st.title("Top 5 Produkte nach Umsatz im letzten Quartal – aufgeteilt nach Region")

query = """
WITH full_table AS (
    SELECT 
		s.region,
		p.title,
		d.year_quarter,
		d.date, 
		f.revenue 
    FROM gold_fact_sales f
    JOIN gold_dim_stores s   ON f.store_id = s.store_id
    JOIN gold_dim_products p ON f.product_id = p.product_id
    JOIN gold_dim_date d     ON f.date_id = d.date_id
), ranked_table as (
SELECT
        region,
        title AS product,
        SUM(revenue) AS total_revenue,
        RANK() OVER (PARTITION BY region ORDER BY SUM(revenue) DESC) AS rnk
    FROM full_table 
    WHERE year_quarter = (
	    SELECT year_quarter
	    FROM full_table
	    ORDER BY date DESC
	    LIMIT 1)
    GROUP BY region, title
)
SELECT *
FROM ranked_table
WHERE rnk <= 5;
"""
df1 = pd.read_sql(query, conn)
st.dataframe(df1)

st.title("Kundeneinteilung")

query2 = """
WITH groups as (
SELECT
	c.name as customer,
    SUM(revenue) AS total_revenue,
    NTILE(3) OVER (ORDER BY SUM(revenue) DESC) AS value_segment
FROM gold_fact_sales f
JOIN gold_dim_date d ON f.date_id = d.date_id
Join gold_dim_customers c on c.customer_id  = f.customer_id  
WHERE d.date >= DATE('2025-01-08','-12 months') -- TODO: dynamisch
GROUP BY f.customer_id
)
Select customer, total_revenue,
CASE
    WHEN value_segment = 1 THEN "High"
    WHEN value_segment = 2 THEN "Mid"
    WHEN value_segment = 3 THEN "Low Value"
    ELSE "No Group"
END as "Group"
from groups;
"""
df2 = pd.read_sql(query2, conn)
st.dataframe(df2)

st.title("Top-10-Produktpaare mit ihrer gemeinsamen Auftrittsfrequenz")

query3 = """
With correlation AS (
SELECT
    CASE 
        WHEN i1.product_id < i2.product_id 
        THEN i1.product_id 
        ELSE i2.product_id 
    END AS product_a,
    CASE 
        WHEN i1.product_id < i2.product_id 
        THEN i2.product_id 
        ELSE i1.product_id 
    END AS product_b,
    COUNT(*) AS freq
FROM gold_fact_sales i1
JOIN gold_fact_sales i2 
    ON i1.order_id = i2.order_id 
   AND i1.product_id < i2.product_id
GROUP BY product_a, product_b
ORDER BY freq DESC
LIMIT 10
)
Select 
 gdp.title as product_a,
 gdp1.title as product_b,
 c.freq 
from correlation c
join gold_dim_products gdp on gdp.product_id = c.product_a
join gold_dim_products gdp1 on gdp1.product_id = c.product_b;
"""
df3 = pd.read_sql(query3, conn)
st.dataframe(df3)

st.title("Anteil der Kunden, die in mehr als einem Quartal bestellt haben")
query4 = """
WITH customer_quarters AS (
    SELECT customer_id, COUNT(DISTINCT quarter) AS quarters_ordered
    FROM gold_fact_sales f
    JOIN gold_dim_date d ON f.date_id = d.date_id
    GROUP BY customer_id
)
SELECT
    100.0 * SUM(CASE WHEN quarters_ordered > 1 THEN 1 ELSE 0 END) / COUNT(*)
    AS pct_multi_quarter_customers
FROM customer_quarters;
"""
df4 = pd.read_sql(query4, conn)
st.dataframe(df4)


st.title("5 Produkt-Verdrängung ")

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
    corr_matrix = pivot.corr()
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



query_bonus = """
SELECT
    d.date,
    d.year,
    d.quarter,
    d.month,
    d.year_quarter,
    p.category,
    p.title AS product,
    SUM(f.revenue) AS revenue
FROM gold_fact_sales f
JOIN gold_dim_products p ON f.product_id = p.product_id
JOIN gold_dim_date d ON f.date_id = d.date_id
GROUP BY d.date, p.title
ORDER BY d.date
"""

df_bonus = pd.read_sql(query_bonus, conn)



df_bonus["date"] = pd.to_datetime(df_bonus["date"])

products = st.multiselect(
    "Produkte auswählen:",
    sorted(df_bonus["product"].unique()),
    default=sorted(df_bonus["product"].unique())[:2]
)

if products:
    filtered_df = df_bonus[df_bonus["product"].isin(products)]


time_level = st.radio(
    "Zeitebene:",
    ["Täglich", "Monatlich", "Quartalsweise", "Jährlich"]
)

if time_level == "Monatlich":
    df_bonus["time"] = df_bonus["date"].dt.to_period("M").astype(str)
elif time_level == "Quartalsweise":
    df_bonus["time"] = df_bonus["year_quarter"]
elif time_level == "Jährlich":
    df_bonus["time"] = df_bonus["year"].astype(str)
else:
    df_bonus["time"] = df_bonus["date"]

plot_df = (
    df_bonus
    .groupby(["time", "product"])["revenue"]
    .sum()
    .reset_index()
)

fig = px.line(
    plot_df,
    x="time",
    y="revenue",
    color="product",
    markers=True,
    title="Umsatzentwicklung"
)

st.plotly_chart(fig, use_container_width=True)


conn.close()
