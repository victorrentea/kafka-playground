package victor.kafka.notification;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import victor.kafka.Events.TeleportFailed;

@SpringBootApplication
public class NotificationService {
  public static void main(String[] args) {
    SpringApplication.run(NotificationService.class, args);
  }

  @KafkaListener(topics = "TeleportFailed", groupId = "notification")
  void onTeleportFailed(TeleportFailed event) {
    System.out.println("📧 Email/SMS to user " + event.userId() + ": sorry, book " + event.bookId() + " could not be teleported");
  }
}
