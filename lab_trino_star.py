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

print("--- 1. Création des Dimensions ---")

# Dimension Utilisateur avec clé surrogate
sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_user AS
    SELECT
        row_number() OVER (ORDER BY user_id) AS user_key,
        user_id, username, name, sex, birthdate, city, state, zip_code
    FROM lakehouse.silver.users
""")

# Dimension Produit avec clé surrogate
sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_product AS
    SELECT row_number() OVER (ORDER BY product) AS product_key, product
    FROM (SELECT DISTINCT product FROM lakehouse.silver.orders)
""")

# Dimension Date
first_day, last_day = sql("""
    SELECT CAST(min(ordered_at) AS date) AS first_day,
           CAST(max(ordered_at) AS date) AS last_day
    FROM lakehouse.silver.orders
""").iloc[0]

sql(f"""
    CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_date AS
    SELECT
        CAST(date_format(CAST(d AS timestamp), '%Y%m%d') AS integer) AS date_key,
        d                                       AS full_date,
        year(d)                                 AS year,
        quarter(d)                              AS quarter,
        month(d)                                AS month,
        date_format(CAST(d AS timestamp), '%M') AS month_name,
        day_of_week(d)                          AS day_of_week,
        day_of_week(d) IN (6, 7)                AS is_weekend
    FROM UNNEST(sequence(DATE '{first_day}', DATE '{last_day}', INTERVAL '1' DAY)) AS t(d)
""")

print("--- 2. Création de la Table de Faits (fact_orders) ---")
sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold.fact_orders AS
    SELECT
        o.order_id,
        u.user_key,
        p.product_key,
        CAST(date_format(o.ordered_at, '%Y%m%d') AS integer) AS date_key,
        o.ordered_at,
        o.quantity
    FROM lakehouse.silver.orders AS o
    LEFT JOIN lakehouse.gold.dim_user    AS u ON u.user_id = o.user_id
    LEFT JOIN lakehouse.gold.dim_product AS p ON p.product = o.product
""")

print("--- 3. Validation de l'intégrité du Schéma en Étoile ---")
silver_orders = sql("SELECT count(*) AS v FROM lakehouse.silver.orders").iloc[0, 0]
fact_orders = sql("SELECT count(*) AS v FROM lakehouse.gold.fact_orders").iloc[0, 0]
silver_units = sql("SELECT sum(quantity) AS v FROM lakehouse.silver.orders").iloc[0, 0]
fact_units = sql("SELECT sum(quantity) AS v FROM lakehouse.gold.fact_orders").iloc[0, 0]

assert silver_orders == fact_orders, f"{silver_orders} != {fact_orders}"
assert silver_units == fact_units, f"{silver_units} != {fact_units}"

unmatched = sql("""
    SELECT count(*) AS v
    FROM lakehouse.gold.fact_orders AS f
    LEFT JOIN lakehouse.gold.dim_user    AS u ON u.user_key    = f.user_key
    LEFT JOIN lakehouse.gold.dim_product AS p ON p.product_key = f.product_key
    LEFT JOIN lakehouse.gold.dim_date    AS d ON d.date_key    = f.date_key
    WHERE u.user_key IS NULL OR p.product_key IS NULL OR d.date_key IS NULL
""").iloc[0, 0]

assert unmatched == 0, f"{unmatched} orders without a complete dimension row"
print(f"[SUCCESS] Intégrité de la table de faits validée : {fact_orders} lignes et {fact_units} unités.")

conn.close()
