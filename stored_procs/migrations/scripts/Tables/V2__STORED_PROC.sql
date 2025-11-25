
CREATE OR REPLACE PROCEDURE {{ DATABASE }}.{{ SCHEMA }}.SP_INIT_CUSTOMER_DATA()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_customer_table STRING DEFAULT '{{ CUSTOMER_TABLE }}';
    v_retailers_table STRING DEFAULT '{{ RETAILERS_TABLE }}';
    v_orders_table STRING DEFAULT '{{ ORDERS_TABLE }}';
    v_log_table STRING DEFAULT '{{ LOG_TABLE }}';
    v_default_status STRING DEFAULT '{{ DEFAULT_STATUS }}';
    v_created_by STRING DEFAULT '{{ CREATED_BY }}';
BEGIN

    -- Create Customer table
    EXECUTE IMMEDIATE
        'CREATE OR REPLACE TABLE ' || '{{ DATABASE }}.{{ SCHEMA }}.' || v_customer_table || ' (
            CUSTOMER_ID INT,
            NAME STRING,
            EMAIL STRING,
            CREATED_AT TIMESTAMP
        )';

    -- Create Retailers Table
    EXECUTE IMMEDIATE
        'CREATE OR REPLACE TABLE ' || '{{ DATABASE }}.{{ SCHEMA }}.' || v_retailers_table || ' (
            RETAILER_ID INT,
            RETAILER_NAME STRING,
            LOCATION STRING,
            CREATED_AT TIMESTAMP
        )';

    -- Create Orders Table
    EXECUTE IMMEDIATE
        'CREATE OR REPLACE TABLE ' || '{{ DATABASE }}.{{ SCHEMA }}.' || v_orders_table || ' (
            ORDER_ID INT,
            CUSTOMER_ID INT,
            RETAILER_ID INT,
            AMOUNT NUMBER(10,2),
            STATUS STRING DEFAULT ''' || v_default_status || ''',
            CREATED_AT TIMESTAMP
        )';

    -- Create Log Table
    EXECUTE IMMEDIATE
        'CREATE OR REPLACE TABLE ' || '{{ DATABASE }}.{{ SCHEMA }}.' || v_log_table || ' (
            LOG_ID INT AUTOINCREMENT,
            MESSAGE STRING,
            CREATED_BY STRING DEFAULT ''' || v_created_by || ''',
            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )';

    RETURN 'All Tables Created Successfully in {{ DATABASE }}.{{ SCHEMA }}';

END;
$$;
