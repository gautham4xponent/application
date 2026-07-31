CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.${table}
(
    eventId STRING,
    eventOffset LONG,
    eventPublisher STRING,
    customerId STRING,
    data STRUCT<
        device_id: STRING,
        device_type: STRING,
        location: STRING,
        event_status: STRING
    >,
    eventTime TIMESTAMP
)
USING DELTA;