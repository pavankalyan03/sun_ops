
CREATE OR REPLACE PROCEDURE {{ target_database }}.{{ target_schema }}.{{ client_prefix }}_PROCESS_PAYMENTS()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
var sql = `
    INSERT INTO {{ target_database }}.{{ target_schema }}.{{ client_prefix }}_PAYMENTS
    SELECT 
        PAYMENT_ID,
        AMOUNT,
        STATUS,
        CURRENT_TIMESTAMP() AS PROCESSED_AT,
        '{{ special_flag }}' AS CLIENT_FLAG
    FROM {{ target_database }}.{{ target_schema }}.{{ client_prefix }}_RAW_PAYMENTS
`;

snowflake.execute({ sqlText: sql });
return 'Client1 payments processed using flag: {{ special_flag }}';
$$;
