CREATE TABLE IF NOT EXISTS Sandbox.Lakshmi.zerobus_device_data
(
    eventId STRING,
    eventOffset BIGINT,
    eventPublisher STRING,
    customerId STRING,
    data STRUCT<
        devices ARRAY<STRUCT<
            deviceId STRING,
            deviceType STRING,
            deviceStatus STRING,
            temperature DOUBLE,
            batteryLevel DOUBLE
>>
>,
    eventTime TIMESTAMP,
    streamStartTime STRING
)
USING DELTA;