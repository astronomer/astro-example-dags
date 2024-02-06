SELECT {cols} FROM FLATTEN(transactionitems)
WHERE
    updatedAt >= CAST('{{ prev_data_interval_start_success }}' AS BSON_DATE)
    AND updatedAt <= CAST('{{ data_interval_end }}' AS BSON_DATE)
ORDER BY updatedAt
LIMIT {limit}
OFFSET {offset}
