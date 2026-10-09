SELECT
    uuid AS order_uuid,
    user_uuid,
    CAST(date AS TIMESTAMP) AS order_timestamp,
    quantity,
    product
FROM read_csv_auto('s3://' || '{{ var("lab_bucket_name") }}' || '/bronze/orders.csv')
