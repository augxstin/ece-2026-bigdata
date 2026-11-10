import os
import pandas as pd
import trino

CATALOG = "lakehouse"
TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
TRINO_USER = os.environ.get("TRINO_USER", os.environ.get("KUBERNETES_NAMESPACE", "default"))

conn = trino.dbapi.connect(
    host=TRINO_HOST,
    port=TRINO_PORT,
    user=TRINO_USER,
    http_scheme="http",
    catalog=CATALOG,
)

def sql(statement):
    cur = conn.cursor()
    cur.execute(statement)
    rows = cur.fetchall()
    columns = [c[0] for c in cur.description] if cur.description else []
    return pd.DataFrame(rows, columns=columns)

print("--- 1. Verification Trino & Catalogs ---")
print("Trino Version:", sql("SELECT version()").iloc[0, 0])
print(sql("SHOW CATALOGS"))
print(sql("SHOW SCHEMAS FROM lakehouse"))
print(sql("SHOW TABLES FROM lakehouse.silver"))

print("\n--- 2. Exploration de la couche Silver ---")
print("Orders count:", sql("SELECT count(*) AS orders FROM lakehouse.silver.orders").iloc[0, 0])
print("Users count:", sql("SELECT count(*) AS users FROM lakehouse.silver.users").iloc[0, 0])

print("\n--- 3. Définition de la couche Gold ---")
sql("CREATE SCHEMA IF NOT EXISTS lakehouse.gold")

# Orders by state and month
sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold.orders_by_state_month AS
    SELECT
        u.state,
        date_trunc('month', o.ordered_at) AS month,
        count(*)                          AS orders,
        sum(o.quantity)                   AS units,
        count(DISTINCT o.user_id)         AS buyers
    FROM lakehouse.silver.orders AS o
    JOIN lakehouse.silver.users  AS u ON u.user_id = o.user_id
    GROUP BY 1, 2
""")

# Top products
sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold.top_products AS
    SELECT
        product,
        count(*)      AS orders,
        sum(quantity) AS units,
        rank() OVER (ORDER BY sum(quantity) DESC) AS units_rank
    FROM lakehouse.silver.orders
    GROUP BY product
""")

# User activity
sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold.user_activity AS
    SELECT
        u.user_id,
        u.state,
        date_diff('year', u.birthdate, current_date) AS age,
        count(o.order_id)        AS orders,
        coalesce(sum(o.quantity), 0) AS units,
        min(o.ordered_at)        AS first_order_at,
        max(o.ordered_at)        AS last_order_at
    FROM lakehouse.silver.users AS u
    LEFT JOIN lakehouse.silver.orders AS o ON o.user_id = u.user_id
    GROUP BY u.user_id, u.state, u.birthdate
""")

print("\n--- 4. Validation des métriques Gold vs Silver ---")
silver_units = sql("SELECT sum(quantity) AS v FROM lakehouse.silver.orders").iloc[0, 0]
gold_units = sql("SELECT sum(units) AS v FROM lakehouse.gold.orders_by_state_month").iloc[0, 0]
product_units = sql("SELECT sum(units) AS v FROM lakehouse.gold.top_products").iloc[0, 0]

print(f"Units Check: Silver={silver_units}, GoldState={gold_units}, GoldProduct={product_units}")
assert silver_units == gold_units
assert silver_units == product_units

print("\n--- 5. Refresh et Time Travel sur top_products ---")
sql("DELETE FROM lakehouse.gold.top_products")
sql("""
    INSERT INTO lakehouse.gold.top_products
    SELECT product, count(*), sum(quantity),
           rank() OVER (ORDER BY sum(quantity) DESC)
    FROM lakehouse.silver.orders
    GROUP BY product
""")

snapshots = sql("SELECT snapshot_id, committed_at, operation FROM lakehouse.gold.\"top_products\" ORDER BY committed_at")
print(snapshots)

print("\n[OK] Validation du Lab 2 Trino terminée avec succès.")
