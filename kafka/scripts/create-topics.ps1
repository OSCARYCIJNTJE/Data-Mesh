$KAFKA_CONTAINER = "kafka-kafka-1"

$topics = @(
  "deliveries",
  "equipment-events",
  "personnel-events",
  "planning-events",
  "site-events",
  "correlation-events",
  "risk-events",
  "alerts",
  "site-health-stream"
)

Write-Host "Creating topics..."

foreach ($topic in $topics) {
  docker exec -it $KAFKA_CONTAINER /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server kafka:29092 `
    --create --if-not-exists `
    --topic $topic `
    --partitions 1 `
    --replication-factor 1
}

Write-Host "Topics created."