


CREATE OR REPLACE PROCEDURE {{ target_database }}.{{ target_schema }}.{{ client_prefix }}_UPDATE_CUSTOMERS()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
var sql = `
    UPDATE {{ target_database }}.{{ target_schema }}.{{ client_prefix }}_CUSTOMERS
    SET TIER = 
        CASE 
            WHEN TOTAL_REVENUE > {{ tier_gold_threshold }} THEN 'GOLD'
            WHEN TOTAL_REVENUE > {{ tier_silver_threshold }} THEN 'SILVER'
            ELSE 'BRONZE'
        END
`;



snowflake.execute({ sqlText: sql });
return 'Client2 customers updated using revenue thresholds.';
$$;
