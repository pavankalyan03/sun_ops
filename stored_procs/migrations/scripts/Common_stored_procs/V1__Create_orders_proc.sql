
CREATE OR REPLACE PROCEDURE {{ target_database }}.{{ target_schema }}.CREATE_ORDERS_TABLE()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
var sql = `
    CREATE TABLE IF NOT EXISTS {{ target_database }}.{{ target_schema }}.{{ client_prefix }}_ORDERS (
        ORDER_ID VARCHAR,
        CUSTOMER_ID VARCHAR,
        AMOUNT NUMBER,
        CREATED_AT TIMESTAMP
    )
`;

snowflake.execute({ sqlText: sql });

return 'Orders table created successfully for client prefix: {{ client_prefix }}';
$$;
