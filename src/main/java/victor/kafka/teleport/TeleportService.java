package victor.kafka.teleport;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import victor.kafka.Events.TeleportBooksToUser;
import victor.kafka.Events.TeleportFailed;
import victor.kafka.Events.UserAddressUpdated;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Start it twice: the 2 instances split the partitions of TeleportBooksToUser (competing consumers). */
@SpringBootApplication
public class TeleportService {
  public static void main(String[] args) {
    SpringApplication.run(TeleportService.class, args);
  }

  @Autowired KafkaTemplate<Integer, Object> kafka;
  Map<Integer, String> addressOfUser = new ConcurrentHashMap<>(); // read projection of User's data

  // a new group on every start ⇒ EACH instance replays ALL addresses from offset 0
  @KafkaListener(topics = "UserAddressUpdated", groupId = "teleport-${random.uuid}")
  void onAddress(UserAddressUpdated event) {
    addressOfUser.put(event.userId(), event.address());
  }

  @KafkaListener(topics = "TeleportBooksToUser", groupId = "teleport")
  void teleport(TeleportBooksToUser command) {
    String address = addressOfUser.get(command.userId());
    for (int bookId : command.bookIds()) {
      if (address == null) {
        System.out.println("💥 Book " + bookId + " teleport failed: no address for user " + command.userId());
        kafka.send("TeleportFailed", command.userId(), new TeleportFailed(bookId, command.userId()));
      } else {
        System.out.println("✨ Book " + bookId + " teleported to " + address);
      }
    }
  }
}
