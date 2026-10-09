SELECT
    uuid AS user_uuid,
    username,
    name,
    sex,
    address,
    mail AS email,
    CAST(birthdate AS DATE) AS birthdate
FROM read_csv_auto('s3://' || '{{ var("lab_bucket_name") }}' || '/bronze/users.csv')
