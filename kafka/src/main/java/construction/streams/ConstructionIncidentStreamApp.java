package construction.streams;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;

import com.fasterxml.jackson.databind.ObjectMapper;

import construction.AlertEvent;
import construction.SiteIncidentEvent;

public class ConstructionIncidentStreamApp {

    public static KafkaStreams start() {
        
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "construction-incident-engine");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        ObjectMapper mapper = new ObjectMapper();

        // ======================================================
        // DELIVERY STREAM
        // ======================================================
        KStream<String, String> deliveries = builder.stream("deliveries");

        KStream<String, String> deliveryIncidents = deliveries
                .filter((k, v) -> v.contains("DELAYED"))
                .mapValues(v -> createIncident(
                        "DELIVERY_DELAY",
                        "HIGH",
                        v,
                        mapper
                ));

        // ======================================================
        // ALERT LOGIC (WINDOWED DETECTION)
        // ======================================================
        KStream<String, String> delayedDeliveries =
                deliveries.filter((k, v) -> v.contains("DELAYED"));

        delayedDeliveries
                .groupBy(
                        (key, value) -> "SITE-A",
                        Grouped.with(Serdes.String(), Serdes.String())
                )
                .windowedBy(
                        TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(10))
                )
                .count()
                .toStream()
                .filter((windowedKey, count) -> count >= 3)
                .map((windowedKey, count) -> {

                    try {
                        AlertEvent alert = new AlertEvent(
                                "REPEATED_DELIVERY_DELAYS",
                                windowedKey.key(),
                                "HIGH",
                                count,
                                System.currentTimeMillis()
                        );

                        String json = mapper.writeValueAsString(alert);

                        return KeyValue.pair(windowedKey.key(), json);

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .to(
                        "alerts",
                        Produced.with(Serdes.String(), Serdes.String())
                );

        // ======================================================
        // EQUIPMENT STREAM
        // ======================================================
        KStream<String, String> equipment = builder.stream("equipment-events");

        KStream<String, String> equipmentIncidents = equipment
                .filter((k, v) -> v.contains("MALFUNCTION"))
                .mapValues(v -> createIncident(
                        "EQUIPMENT_FAILURE",
                        "CRITICAL",
                        v,
                        mapper
                ));

        // ======================================================
        // PERSONNEL STREAM
        // ======================================================
        KStream<String, String> personnel = builder.stream("personnel-events");

        KStream<String, String> personnelIncidents = personnel
                .filter((k, v) -> v.contains("INJURED"))
                .mapValues(v -> createIncident(
                        "PERSONNEL_INJURY",
                        "HIGH",
                        v,
                        mapper
                ));

        // ======================================================
        // MERGE INCIDENTS
        // ======================================================
        KStream<String, String> allIncidents =
                deliveryIncidents
                        .merge(equipmentIncidents)
                        .merge(personnelIncidents);

        allIncidents.to(
                "site-events",
                Produced.with(Serdes.String(), Serdes.String())
        );

        // ======================================================
        // START STREAMS
        // ======================================================
        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        streams.start();

        System.out.println("🏗️ Construction Incident Stream Engine started");

        return streams;
    }

    // ======================================================
    // INCIDENT CREATOR
    // ======================================================
    private static String createIncident(
            String type,
            String severity,
            String originalEvent,
            ObjectMapper mapper
    ) {
        try {
            SiteIncidentEvent incident = new SiteIncidentEvent(
                    UUID.randomUUID().toString(),
                    "SITE-A",
                    System.currentTimeMillis(),
                    type,
                    severity,
                    originalEvent
            );

            return mapper.writeValueAsString(incident);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}