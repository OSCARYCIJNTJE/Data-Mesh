package construction;

import construction.streams.ConstructionIncidentStreamApp;
import construction.streams.ConstructionCorrelationStreamApp;
import construction.streams.ConstructionRiskStreamApp;
import construction.streams.ConstructionAlertStreamApp;
import construction.producer.ConstructionEventProducer;

import org.apache.kafka.streams.KafkaStreams;

import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) {

        System.out.println("🏗️ Starting Construction Intelligence Platform...");
        new Thread(() -> {
            try {
                ConstructionEventProducer.main(new String[]{});
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).start();

        List<KafkaStreams> streamsApps = new ArrayList<>();

        streamsApps.add(ConstructionIncidentStreamApp.start());
        streamsApps.add(ConstructionCorrelationStreamApp.start());
        streamsApps.add(ConstructionRiskStreamApp.start());
        streamsApps.add(ConstructionAlertStreamApp.start());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("🛑 Shutting down all stream apps...");
            streamsApps.forEach(KafkaStreams::close);
        }));

        System.out.println("✅ All stream apps started");
    }
}