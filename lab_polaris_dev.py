import os
from pyspark.sql import SparkSession

POLARIS_PRINCIPAL = "dev"
POLARIS_CREDENTIAL = os.environ.get("POLARIS_DEV_CREDENTIAL", "dev:secret")
BUCKET = os.environ.get("LAB_BUCKET_NAME", os.environ.get("KUBERNETES_NAMESPACE", "default"))
POLARIS_URI = os.environ.get("POLARIS_URI", "http://polaris:8181")
POLARIS_CATALOG = os.environ.get("POLARIS_CATALOG", BUCKET)

ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4"

spark = (
    SparkSession.builder
    .appName("lab-polaris-dev")
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
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
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

print("--- 1. Verification des namespaces ---")
spark.sql("SHOW NAMESPACES IN lab").show()
spark.sql("SHOW NAMESPACES IN polaris").show()

print("--- 2. Enregistrement de la table Silver orders dans Polaris ---")
spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.silver")
spark.sql(f"""
    CALL polaris.system.register_table(
      table => 'silver.orders',
      metadata_file => 's3a://{BUCKET}/warehouse/silver/orders/metadata/v1.metadata.json'
    )
""").show()
spark.sql("SELECT count(*) AS orders FROM polaris.silver.orders").show()

print("--- 3. Creation de la table silver.users geree par Polaris ---")
stg_users = spark.table("lab.silver.users")
stg_users.writeTo("polaris.silver.users").createOrReplace()
spark.sql("SELECT count(*) AS users FROM polaris.silver.users").show()

print("--- 4. Evolution du schema comme DEV ---")
spark.sql("ALTER TABLE polaris.silver.users ADD COLUMN country STRING")
spark.sql("DESCRIBE polaris.silver.users").show()

print("--- 5. Exercice 1 : Suppression de colonne et snapshots ---")
spark.sql("ALTER TABLE polaris.silver.users DROP COLUMN country")
spark.sql("SELECT snapshot_id, operation, committed_at FROM polaris.silver.users.snapshots").show(truncate=False)

spark.stop()
