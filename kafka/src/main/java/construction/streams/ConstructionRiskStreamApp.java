package construction.streams;

import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import com.fasterxml.jackson.databind.ObjectMapper;

import construction.RiskEvent;

public class ConstructionRiskStreamApp {

    public static KafkaStreams start() {

        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

        // ======================================================
        // CONFIG
        // ======================================================
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "construction-risk-engine");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        ObjectMapper mapper = new ObjectMapper();

        // ======================================================
        // INPUT: Correlation Events
        // ======================================================
        KStream<String, String> correlations =
                builder.stream("correlation-events");

        // ======================================================
        // RISK SCORING ENGINE
        // ======================================================
        KStream<String, String> riskStream =
                correlations.mapValues(value -> {

                    try {

                        int score;
                        String level;
                        String sourceType = "CORRELATION";
                        String description;

                        // ==================================================
                        // SIMPLE RULE-BASED SCORING
                        // ==================================================
                        if (value.contains("SAFETY_ESCALATION")) {
                            score = 100;
                            level = "CRITICAL";
                            description = "Equipment failure + personnel injury detected";
                        } else if (value.contains("SITE_DISRUPTION")) {
                            score = 70;
                            level = "HIGH";
                            description = "Delivery delay + equipment issue detected";
                        } else {
                            score = 40;
                            level = "MEDIUM";
                            description = "General operational risk detected";
                        }

                        // ==================================================
                        // CREATE RISK EVENT
                        // ==================================================
                        RiskEvent risk = new RiskEvent(
                                UUID.randomUUID().toString(),
                                "SITE-A",
                                score,
                                level,
                                sourceType,
                                description,
                                System.currentTimeMillis()
                        );

                        return mapper.writeValueAsString(risk);

                    } catch (Exception e) {
                        return null;
                    }
                });

        // ======================================================
        // OUTPUT: risk-events topic
        // ======================================================
        riskStream.to(
                "risk-events",
                Produced.with(Serdes.String(), Serdes.String())
        );

        // ======================================================
        // START STREAM
        // ======================================================
        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        streams.start();

        System.out.println("⚠️ Phase 7 Risk Engine started");

        return streams;
    }
}