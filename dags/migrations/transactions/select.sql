SELECT {cols} FROM FLATTEN(transactions)
WHERE
    updatedAt >= CAST('{{ ds }}' AS BSON_DATE)
    AND updatedAt < CAST('{{ next_ds }}' AS BSON_DATE)
ORDER BY updatedAt
LIMIT {limit}
OFFSET {offset}
