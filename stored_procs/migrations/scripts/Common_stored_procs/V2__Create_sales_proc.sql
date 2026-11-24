CREATE OR REPLACE PROCEDURE {{ target_database }}.{{ target_schema }}.CREATE_SALES_TABLE()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
var sql = `
    CREATE TABLE IF NOT EXISTS {{ target_database }}.{{ target_schema }}.{{ client_prefix }}_SALES (
        ID VARCHAR,
        CUSTOMER_ID VARCHAR,
        AMOUNT NUMBER,
        CREATED_AT TIMESTAMP
    )
`;

snowflake.execute({ sqlText: sql });

return 'SALES table created successfully for client prefix: {{ client_prefix }}';
$$;
