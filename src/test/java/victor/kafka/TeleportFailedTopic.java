package victor.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Reads the TeleportFailed topic from the start. Call {@link #eventsOf} repeatedly, e.g. from Awaitility. */
class TeleportFailedTopic implements AutoCloseable {
    private final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
            "bootstrap.servers", "localhost:9092",
            "group.id", "test-" + UUID.randomUUID(), // new group ⇒ reads from offset 0
            "auto.offset.reset", "earliest",
            "key.deserializer", StringDeserializer.class,
            "value.deserializer", StringDeserializer.class));
    private final List<ConsumerRecord<String, String>> received = new ArrayList<>();

    TeleportFailedTopic() {
        consumer.subscribe(List.of("TeleportFailed"));
    }

    /** Polls once for new events, then returns all events received so far about this user (key = userId) */
    List<String> eventsOf(int userId) {
        consumer.poll(Duration.ofMillis(200)).forEach(received::add);
        return received.stream()
                .filter(record -> String.valueOf(userId).equals(record.key()))
                .map(ConsumerRecord::value)
                .toList();
    }

    @Override
    public void close() {
        consumer.close();
    }
}
