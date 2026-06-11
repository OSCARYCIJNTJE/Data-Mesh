package construction.streams;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import construction.CorrelationEvent;

public class ConstructionCorrelationStreamApp {

    public static KafkaStreams start() {
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        // ======================================================
        // CONFIG
        // ======================================================
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "construction-correlation-engine");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        ObjectMapper mapper = new ObjectMapper();

        // ======================================================
        // INPUT STREAMS
        // ======================================================
        KStream<String, String> deliveries = builder.stream("deliveries");
        KStream<String, String> equipment = builder.stream("equipment-events");
        KStream<String, String> personnel = builder.stream("personnel-events");

        // ======================================================
        // STEP 1: NORMALIZE EVENTS (FILTER SIGNALS)
        // ======================================================

        KStream<String, String> deliverySignals =
                deliveries.filter((k, v) -> v.contains("DELAYED"));

        KStream<String, String> equipmentSignals =
                equipment.filter((k, v) -> v.contains("MALFUNCTION"));

        KStream<String, String> personnelSignals =
                personnel.filter((k, v) -> v.contains("INJURED"));

        // ======================================================
        // STEP 2: SET KEY = SITE (IMPORTANT FOR JOINS)
        // ======================================================

        KStream<String, String> deliveryBySite =
                deliverySignals.selectKey((k, v) -> "SITE-A");

        KStream<String, String> equipmentBySite =
                equipmentSignals.selectKey((k, v) -> "SITE-A");

        KStream<String, String> personnelBySite =
                personnelSignals.selectKey((k, v) -> "SITE-A");

        // ======================================================
        // STEP 3: WINDOW CONFIGURATION
        // ======================================================
        JoinWindows window = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5));

        // ======================================================
        // CORRELATION 1:
        // DELIVERY + EQUIPMENT → SITE DISRUPTION
        // ======================================================
        KStream<String, String> deliveryEquipmentCorrelation =
                deliveryBySite.join(
                        equipmentBySite,
                        (deliveryEvent, equipmentEvent) -> buildCorrelationEvent(
                                "SITE_DISRUPTION",
                                "HIGH",
                                deliveryEvent,
                                equipmentEvent,
                                "Delivery delay + equipment issue detected",
                                mapper
                        ),
                        window
                );

        // ======================================================
        // CORRELATION 2:
        // EQUIPMENT + PERSONNEL → SAFETY ESCALATION
        // ======================================================
        KStream<String, String> safetyCorrelation =
                equipmentBySite.join(
                        personnelBySite,
                        (equipmentEvent, personnelEvent) -> buildCorrelationEvent(
                                "SAFETY_ESCALATION",
                                "CRITICAL",
                                equipmentEvent,
                                personnelEvent,
                                "Equipment failure + personnel injury detected",
                                mapper
                        ),
                        window
                );

        // ======================================================
        // STEP 4: MERGE ALL CORRELATIONS
        // ======================================================
        KStream<String, String> allCorrelations =
                deliveryEquipmentCorrelation.merge(safetyCorrelation);

        // ======================================================
        // STEP 5: OUTPUT
        // ======================================================
        allCorrelations.to(
                "correlation-events",
                Produced.with(Serdes.String(), Serdes.String())
        );

        // ======================================================
        // START STREAMS
        // ======================================================
        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        streams.start();

        System.out.println("🔗 Phase 6 Correlation Engine started");

        return streams;
    }

    private static String buildCorrelationEvent(
            String type,
            String severity,
            String eventA,
            String eventB,
            String message,
            ObjectMapper mapper
    ) {
        try {
            CorrelationEvent event = new CorrelationEvent(
                    UUID.randomUUID().toString(),
                    "SITE-A",
                    type,
                    severity,
                    message,
                    eventA,
                    eventB,
                    System.currentTimeMillis()
            );

            return mapper.writeValueAsString(event);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}