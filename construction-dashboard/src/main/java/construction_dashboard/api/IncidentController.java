package construction_dashboard.api;

import construction_dashboard.kafka.IncidentConsumer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class IncidentController {

    private final IncidentConsumer consumer;

    public IncidentController(IncidentConsumer consumer) {
        this.consumer = consumer;
    }

    @GetMapping("/incidents")
    public List<String> getIncidents() {
        return consumer.getIncidents();
    }
}