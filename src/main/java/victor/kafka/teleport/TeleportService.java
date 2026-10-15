package victor.kafka.teleport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import victor.kafka.Events.TeleportBooksToUser;
import victor.kafka.Events.TeleportFailed;
import victor.kafka.Events.UserAddressUpdated;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@SpringBootApplication
public class TeleportService {
    private static final Logger log = LoggerFactory.getLogger(TeleportService.class);

    public static void main(String[] args) {
        SpringApplication.run(TeleportService.class, args);
    }

    @Autowired KafkaTemplate<String, Object> kafka;
    Map<Integer, String> addressOfUser = new ConcurrentHashMap<>(); // read projection of User's data

    // no consumer group: EACH instance reads ALL partitions (num.partitions=2) from offset 0, on every start
    @KafkaListener(topicPartitions = @TopicPartition(topic = "UserAddressUpdated",
            partitionOffsets = @PartitionOffset(partition = "0-1", initialOffset = "0")))
    void onAddress(UserAddressUpdated event) {
        addressOfUser.put(event.userId(), event.address());
    }

    @KafkaListener(topics = "TeleportBooksToUser", groupId = "teleport")
    void teleport(TeleportBooksToUser command) {
        String address = addressOfUser.get(command.userId());
        for (int bookId : command.bookIds()) {
            if (address == null) {
                log.info("💥 Book {} teleport failed: no address for user {}", bookId, command.userId());
                kafka.send("TeleportFailed", String.valueOf(command.userId()), new TeleportFailed(bookId, command.userId()));
            } else {
                log.info("✨ Book {} teleported to {}", bookId, address);
            }
        }
    }
}
