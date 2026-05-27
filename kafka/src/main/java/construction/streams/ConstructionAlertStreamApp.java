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

import construction.AlertEvent;

public class ConstructionAlertStreamApp {

    public static void main(String[] args) {

        // ======================================================
        // CONFIG
        // ======================================================
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "construction-alert-engine");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        ObjectMapper mapper = new ObjectMapper();

        // ======================================================
        // INPUT: Risk Events
        // ======================================================
        KStream<String, String> risks =
                builder.stream("risk-events");

        // ======================================================
        // ALERT LOGIC
        // ======================================================
        KStream<String, String> alerts =
                risks.filter((key, value) -> {

                    // simple string-based filtering (ok for prototype)
                    return value.contains("\"CRITICAL\"")
                            || value.contains("\"riskScore\":100")
                            || value.contains("\"riskScore\":90")
                            || value.contains("\"riskScore\":80");
                })
                .mapValues(value -> {

                    try {
                        long score = 80;

                        AlertEvent alert = new AlertEvent(
                                "RISK_ALERT",
                                "SITE-A",
                                score >= 80 ? "CRITICAL" : "MEDIUM",
                                score,
                                System.currentTimeMillis()
                        );

                        return mapper.writeValueAsString(alert);

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

        // ======================================================
        // OUTPUT
        // ======================================================
        alerts.to(
                "alerts",
                Produced.with(Serdes.String(), Serdes.String())
        );

        // ======================================================
        // START STREAM
        // ======================================================
        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        streams.start();

        System.out.println("🚨 Phase 8 Alert Engine started");

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}