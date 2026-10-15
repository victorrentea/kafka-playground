package victor.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/** Reads the TeleportFailed topic from the start, looking for an event about this user (key = userId). */
class TeleportFailedTopic {
    static Optional<String> waitForEventOf(int userId, Duration timeout) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                "bootstrap.servers", "localhost:9092",
                "group.id", "test-" + UUID.randomUUID(), // new group ⇒ reads from offset 0
                "auto.offset.reset", "earliest",
                "key.deserializer", StringDeserializer.class,
                "value.deserializer", StringDeserializer.class));
        try (consumer) {
            consumer.subscribe(List.of("TeleportFailed"));
            long deadline = System.currentTimeMillis() + timeout.toMillis();
            while (System.currentTimeMillis() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(200))) {
                    if (String.valueOf(userId).equals(record.key())) return Optional.of(record.value());
                }
            }
            return Optional.empty();
        }
    }
}
