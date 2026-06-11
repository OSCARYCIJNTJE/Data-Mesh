CREATE STREAM deliveries_stream (
    eventId STRING,
    eventType STRING,
    siteId STRING,
    timestamp BIGINT,
    material STRING,
    quantity INTEGER,
    status STRING
) WITH (
    KAFKA_TOPIC='deliveries',
    VALUE_FORMAT='JSON'
);

CREATE STREAM equipment_events (
    eventId STRING,
    eventType STRING,
    siteId STRING,
    timestamp BIGINT,
    equipmentId STRING,
    status STRING,
    issue STRING
) WITH (
    KAFKA_TOPIC='equipment-events',
    VALUE_FORMAT='JSON'
);

CREATE STREAM personnel_events (
    eventId STRING,
    eventType STRING,
    siteId STRING,
    timestamp BIGINT,
    personnelId STRING,
    status STRING,
    reason STRING
) WITH (
    KAFKA_TOPIC='personnel-events',
    VALUE_FORMAT='JSON'
);

CREATE STREAM planning_events (
    eventId STRING,
    eventType STRING,
    siteId STRING,
    timestamp BIGINT,
    planType STRING,
    description STRING
) WITH (
    KAFKA_TOPIC='planning-events',
    VALUE_FORMAT='JSON'
);

CREATE STREAM site_incidents (
    incidentId STRING,
    siteId STRING,
    timestamp BIGINT,
    type STRING,
    severity STRING,
    message STRING
) WITH (
    KAFKA_TOPIC='site-events',
    VALUE_FORMAT='JSON'
);

CREATE TABLE delivery_delay_alerts AS
SELECT
    siteId,
    COUNT(*) AS delay_count
FROM deliveries_stream
WINDOW TUMBLING (SIZE 10 MINUTES)
WHERE status = 'DELAYED'
GROUP BY siteId
HAVING COUNT(*) >= 3
EMIT CHANGES;

CREATE STREAM correlation_events_stream (
    correlationId STRING,
    siteId STRING,
    type STRING,
    severity STRING,
    message STRING,
    eventA STRING,
    eventB STRING,
    timestamp BIGINT
) WITH (
    KAFKA_TOPIC='correlation-events',
    VALUE_FORMAT='JSON'
);

CREATE STREAM risk_stream (
    riskId STRING,
    siteId STRING,
    riskScore INTEGER,
    riskLevel STRING,
    sourceType STRING,
    description STRING,
    timestamp BIGINT
) WITH (
    KAFKA_TOPIC='risk-events',
    VALUE_FORMAT='JSON'
);

CREATE STREAM alert_stream (
    alertType STRING,
    siteId STRING,
    severity STRING,
    count BIGINT,
    timestamp BIGINT
) WITH (
    KAFKA_TOPIC='alerts',
    VALUE_FORMAT='JSON'
);

CREATE STREAM risk_normalized AS
SELECT
    siteId,
    'RISK' AS eventType,
    riskLevel AS severity,
    CAST(riskScore AS BIGINT) AS score,
    description,
    timestamp
FROM risk_stream
EMIT CHANGES;

CREATE STREAM correlation_normalized AS
SELECT
    siteId,
    'CORRELATION' AS eventType,
    severity,
    CAST(NULL AS BIGINT) AS score,
    message AS description,
    timestamp
FROM correlation_events_stream
EMIT CHANGES;

CREATE STREAM alert_normalized AS
SELECT
    siteId,
    'ALERT' AS eventType,
    severity,
    CAST(count AS BIGINT) AS score,
    alertType AS description,
    timestamp
FROM alert_stream
EMIT CHANGES;

CREATE STREAM site_health_stream (
    siteId STRING,
    eventType STRING,
    severity STRING,
    score BIGINT,
    description STRING,
    timestamp BIGINT
) WITH (
    KAFKA_TOPIC='site-health-stream',
    VALUE_FORMAT='JSON',
    PARTITIONS=1,
    REPLICAS=1
);

INSERT INTO site_health_stream
SELECT
    siteId,
    eventType,
    severity,
    score,
    description,
    timestamp
FROM risk_normalized
EMIT CHANGES;

INSERT INTO site_health_stream
SELECT
    siteId,
    eventType,
    severity,
    score,
    description,
    timestamp
FROM correlation_normalized
EMIT CHANGES;

INSERT INTO site_health_stream
SELECT
    siteId,
    eventType,
    severity,
    score,
    description,
    timestamp
FROM alert_normalized
EMIT CHANGES;