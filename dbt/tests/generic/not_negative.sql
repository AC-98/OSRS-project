-- Custom test: Check that a column does not contain negative values
SELECT *
FROM {{ ref(model) }}
WHERE {{ column_name }} < 0
