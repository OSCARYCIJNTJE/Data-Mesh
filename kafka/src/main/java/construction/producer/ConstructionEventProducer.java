package construction.producer;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import construction.DeliveryEvent;
import construction.EquipmentEvent;
import construction.PersonnelEvent;
import construction.PlanningEvent;

public class ConstructionEventProducer {

    private static final Random random = new Random();
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        // 1. Kafka config
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        System.out.println("🏗️ Construction Event Simulator started...");

        while (true) {

            int eventType = random.nextInt(4);

            switch (eventType) {

                case 0 -> sendDelivery(producer);
                case 1 -> sendEquipment(producer);
                case 2 -> sendPersonnel(producer);
                case 3 -> sendPlanning(producer);
            }

            Thread.sleep(5000);
        }
    }

    // ---------------- DELIVERY ----------------
    private static void sendDelivery(KafkaProducer<String, String> producer) throws Exception {

        DeliveryEvent event = new DeliveryEvent(
                UUID.randomUUID().toString(),
                "SITE-A",
                Instant.now().toEpochMilli(),
                randomMaterial(),
                random.nextInt(100),
                random.nextBoolean() ? "DELIVERED" : "DELAYED"
        );

        send(producer, "deliveries", event);
    }

    // ---------------- EQUIPMENT ----------------
    private static void sendEquipment(KafkaProducer<String, String> producer) throws Exception {

        EquipmentEvent event = new EquipmentEvent(
                UUID.randomUUID().toString(),
                "SITE-A",
                Instant.now().toEpochMilli(),
                randomEquipment(),
                random.nextBoolean() ? "OK" : "MALFUNCTION",
                "Engine issue detected"
        );

        send(producer, "equipment-events", event);
    }

    // ---------------- PERSONNEL ----------------
    private static void sendPersonnel(KafkaProducer<String, String> producer) throws Exception {

        PersonnelEvent event = new PersonnelEvent(
                UUID.randomUUID().toString(),
                "SITE-A",
                Instant.now().toEpochMilli(),
                randomPerson(),
                random.nextBoolean() ? "ACTIVE" : "INJURED",
                "Safety incident"
        );

        send(producer, "personnel-events", event);
    }

    // ---------------- PLANNING ----------------
    private static void sendPlanning(KafkaProducer<String, String> producer) throws Exception {

        PlanningEvent event = new PlanningEvent(
                UUID.randomUUID().toString(),
                "SITE-A",
                Instant.now().toEpochMilli(),
                "SCHEDULE_CHANGE",
                "Shifted due to weather delay"
        );

        send(producer, "planning-events", event);
    }

    // ---------------- SEND HELPER ----------------
    private static void send(KafkaProducer<String, String> producer,
                             String topic,
                             Object event) throws Exception {

        String json = mapper.writeValueAsString(event);

        producer.send(new ProducerRecord<>(topic, json));

        System.out.println("📤 " + topic + " -> " + json);
    }

    // ---------------- RANDOM HELPERS ----------------
    private static String randomMaterial() {
        String[] items = {"cement", "steel", "bricks", "glass"};
        return items[random.nextInt(items.length)];
    }

    private static String randomEquipment() {
        String[] items = {"crane", "excavator", "bulldozer", "forklift"};
        return items[random.nextInt(items.length)];
    }

    private static String randomPerson() {
        String[] items = {"worker-1", "worker-2", "worker-3"};
        return items[random.nextInt(items.length)];
    }
}
