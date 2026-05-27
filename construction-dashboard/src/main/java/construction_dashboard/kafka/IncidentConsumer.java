package construction_dashboard.kafka;

import java.util.ArrayList;
import java.util.List;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class IncidentConsumer {

    private final List<String> incidents = new ArrayList<>();

    @KafkaListener(topics = "site-events", groupId = "construction-dashboard")
    public void consume(String message) {

        System.out.println("📥 Incident received: " + message);

        incidents.add(message);
    }

    public List<String> getIncidents() {
        return incidents;
    }
}