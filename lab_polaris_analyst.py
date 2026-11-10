import os
from pyspark.sql import SparkSession

POLARIS_PRINCIPAL = "analyst"
POLARIS_CREDENTIAL = os.environ.get("POLARIS_ANALYST_CREDENTIAL", "analyst:secret")
BUCKET = os.environ.get("LAB_BUCKET_NAME", os.environ.get("KUBERNETES_NAMESPACE", "default"))
POLARIS_URI = os.environ.get("POLARIS_URI", "http://polaris:8181")
POLARIS_CATALOG = os.environ.get("POLARIS_CATALOG", BUCKET)

ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4"

spark = (
    SparkSession.builder
    .appName("lab-polaris-analyst")
    .master("local[*]")
    .config("spark.driver.memory", "512m")
    .config("spark.jars.packages", ICEBERG_PACKAGE)
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ.get('AWS_S3_ENDPOINT', 's3.seaweedfs.adm.adaltas.cloud')}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", ""))
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", ""))
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.polaris.type", "rest")
    .config("spark.sql.catalog.polaris.uri", f"{POLARIS_URI}/api/catalog")
    .config("spark.sql.catalog.polaris.warehouse", POLARIS_CATALOG)
    .config("spark.sql.catalog.polaris.credential", POLARIS_CREDENTIAL)
    .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
    .config("spark.sql.catalog.polaris.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print(f"Polaris principal : {POLARIS_PRINCIPAL}")

print("\n--- 1. Verification de la lecture pour Analyst ---")
spark.sql("SHOW TABLES IN polaris.silver").show()
spark.sql("SELECT product, count(*) FROM polaris.silver.orders GROUP BY product").show()

print("\n--- 2. Test d'ecriture INSERT par Analyst (Echec Forbidden) ---")
try:
    spark.sql("INSERT INTO polaris.silver.orders SELECT * FROM polaris.silver.orders LIMIT 1")
    print("ERREUR : L'insertion n'aurait pas du passer !")
except Exception as e:
    print("[RBAC OK] Accès refuse par Polaris en ecriture (Forbidden).")

print("\n--- 3. Test ALTER TABLE par Analyst (Echec Forbidden) ---")
try:
    spark.sql("ALTER TABLE polaris.silver.users ADD COLUMN phone STRING")
    print("ERREUR : L'ALTER TABLE n'aurait pas du passer !")
except Exception as e:
    print("[RBAC OK] Alter table refuse par Polaris (Forbidden).")

spark.stop()
