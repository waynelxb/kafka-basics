from ksql import KSQLAPI

client = KSQLAPI("http://localhost:8088")
# client.get_properties()
# print(client.ksql("show topics;"))
# print(client.ksql("show streams;"))
# print(client.ksql("show tables;"))
# client.ksql("describe <STREAM_NAME> extended;")

sql_drop_objects = """
-- Create Join Stock and Company Materialized Table
DROP TABLE IF EXISTS table_stk_stock_with_fields_tumbling;
DROP STREAM IF EXISTS stream_stk_stock;
--DROP TABLE IF EXISTS ksqltablegroupcompany;
--DROP TABLE IF EXISTS ksqltablegroupstock;
--DROP STREAM IF EXISTS ksqlstreamcompany;
--DROP STREAM IF EXISTS ksqlstreamstock
"""
client.ksql(sql_drop_objects)


# SHOULD NOT PUT ";" AT THE END OF SQL SCRIPT HERE
# Because stk_stock is using schema registry, you DO NOT(in fact SHOULD NOT!!!) need define any columns in the CREATE statement. \
# ksqlDB infers this information automatically from the latest registered schema for the stk_stock topic. 
# ksqlDB uses the most recent schema at the time the statement is first executed. 

sql_create_stream_stk_stock = """
    CREATE STREAM IF NOT EXISTS stream_stk_stock
        WITH (
            KAFKA_TOPIC='stk_stock',
            VALUE_FORMAT='AVRO'
    )
"""
client.ksql(sql_create_stream_stk_stock)
print("sql_create_stream_stk_stock")

# EMIT CHANGES is an optional clause for CTAS and CSAS statement.
# For CSAS/CTAS both queries with and without EMIT CHANGES clause are exactly the same.
# In ksqlDB, the EMIT CHANGES clause in a SELECT statement specifies a push query that emits all changes in real-time to the client: 
# Syntax: For example, SELECT * FROM WORKSHOP_USERS EMIT CHANGES 
# Behavior: The query runs continuously and emits the latest data as it's produced 
# You can also use other output refinement types with the EMIT clause, such as FINAL, which only emits the final result of a windowed aggregation

# In ksqlDB, you might need to use a GROUP BY clause in certain situations, 
# such as when creating tables from other sources, using the WINDOW clause, or aggregating data

sql_create_table_stk_stock_tumbling = """
    --DROP TABLE IF EXISTS table_stk_stock_tumbling
    CREATE OR REPLACE TABLE table_stk_stock_tumbling 
    WITH (KAFKA_TOPIC='table_stk_stock_tumbling', VALUE_FORMAT='AVRO')
    AS
    SELECT
        ticker,
        EARLIEST_BY_OFFSET(datetime) as window_start_datetime,
        LATEST_BY_OFFSET(datetime) as window_end_datetime,
        EARLIEST_BY_OFFSET(high) as window_start_high,
        LATEST_BY_OFFSET(high) as window_end_high,
        EARLIEST_BY_OFFSET(low) as window_start_low,
        LATEST_BY_OFFSET(low) as window_end_low,
        LATEST_BY_OFFSET(close) as window_end_close,
        SUM(volume) as window_total_volume
    FROM stream_stk_stock_with_fields
    WINDOW TUMBLING(SIZE 1 MINUTE)
    GROUP BY ticker
    EMIT CHANGES
"""
client.ksql(sql_create_table_stk_stock_tumbling)



sql_create_stream_stk_company = """
    CREATE STREAM IF NOT EXISTS stream_stk_company
        WITH (
            KAFKA_TOPIC='stk_company',
            VALUE_FORMAT='AVRO'
    )
"""
client.ksql(sql_create_stream_stk_company)
print("sql_create_stream_stk_company")



sql_create_table_stk_company_tumbling = """
    -- Create Company Materialized View to Remove Duplication
    --DROP TABLE IF EXISTS table_stk_company_tumbling;
    CREATE OR REPLACE TABLE table_stk_company_tumbling AS
    SELECT    
        ticker,
        latest_by_offset(name) AS name,
        latest_by_offset(exchange) AS exchange
    FROM stream_stk_company
    WINDOW TUMBLING(SIZE 1 MINUTE)
    GROUP BY id EMIT CHANGES
"""
client.ksql(sql_create_table_stk_company_tumbling)

print("sql_create_table_stk_company_tumbling")


sql_create_stream_join_stk_stock_company = """
-- Create Join Stock and Company Stream Table
DROP STREAM IF EXISTS stream_join_stk_stock_company;
CREATE OR REPLACE STREAM stream_join_stk_stock_company AS
 SELECT
    stmstk.ticker AS ticker,
    stmstk.datetime AS datetime,
    stmstk.open AS open,
    stmstk.high AS high,
    stmstk.low AS low,
    stmstk.close AS close,
    stmstk.volume AS volume,
    stmcmp.name AS name,
    stmcmp.exchange AS logo
 FROM stream_stk_stock stmstk
 INNER JOIN stream_stk_company stmcmp
 WITHIN 1 DAYS GRACE PERIOD 15 MINUTES
 ON stmstk.ticker = stmcmp.ticker EMIT CHANGES
"""
client.ksql(sql_create_stream_join_stk_stock_company)
print("sql_create_stream_join_stk_stock_company")

sql_create_table_join_stk_stock_company = """
-- Create Join Stock and Company Materialized Table
DROP TABLE IF EXISTS table_join_stk_stock_company;
CREATE OR REPLACE TABLE table_join_stk_stock_company AS
    SELECT
        ticker,
        latest_by_offset(datetime) AS datetime,
        latest_by_offset(open) AS open,
        latest_by_offset(high) AS high,
        latest_by_offset(low) AS low,
        latest_by_offset(close) AS close,
        latest_by_offset(volume) AS volume,
        latest_by_offset(name) AS name,
        latest_by_offset(logo) AS exchange
    FROM stream_join_stk_stock_company
    WINDOW TUMBLING (SIZE 1 MINUTE, RETENTION 1 DAYS, GRACE PERIOD 15 MINUTES)
    GROUP BY id EMIT CHANGES
"""
client.ksql(sql_create_table_join_stk_stock_company)
print("sql_create_table_join_stk_stock_company")