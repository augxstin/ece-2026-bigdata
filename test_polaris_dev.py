import os
from pyspark.sql import SparkSession

BUCKET = os.environ.get("KUBERNETES_NAMESPACE", "default")
DEV_ID = os.environ.get("DEV_CLIENT_ID", "dev")
DEV_SECRET = os.environ.get("DEV_CLIENT_SECRET", "secret")

spark = (
    SparkSession.builder
    .appName("polaris-dev-pyspark")
    .master("local[*]")
    .config("spark.driver.memory", "512m")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4")
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ.get('AWS_S3_ENDPOINT', 's3.seaweedfs.adm.adaltas.cloud')}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", ""))
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", ""))
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config(f"spark.sql.catalog.{BUCKET}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{BUCKET}.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
    .config(f"spark.sql.catalog.{BUCKET}.uri", "http://polaris:8181/api/catalog/v1")
    .config(f"spark.sql.catalog.{BUCKET}.oauth2-server-uri", "http://polaris:8181/api/catalog/v1/oauth/tokens")
    .config(f"spark.sql.catalog.{BUCKET}.header.Polaris-Realm", "POLARIS")
    .config(f"spark.sql.catalog.{BUCKET}.credential", f"{DEV_ID}:{DEV_SECRET}")
    .config(f"spark.sql.catalog.{BUCKET}.warehouse", BUCKET)
    .config(f"spark.sql.catalog.{BUCKET}.scope", "PRINCIPAL_ROLE:ALL")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{BUCKET}`.silver")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS `{BUCKET}`.silver.orders (
        order_id STRING,
        quantity INT,
        product STRING
    ) USING iceberg
""")
spark.sql(f"""
    INSERT INTO `{BUCKET}`.silver.orders VALUES 
    ('ord-101', 3, 'cookie'),
    ('ord-102', 5, 'bread')
""")
spark.sql(f"SELECT * FROM `{BUCKET}`.silver.orders").show()
spark.stop()
