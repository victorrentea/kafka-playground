package victor.kafka.notification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import victor.kafka.Events.TeleportFailed;

@SpringBootApplication
public class NotificationService {
    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    public static void main(String[] args) {
        SpringApplication.run(NotificationService.class, args);
    }

    @RetryableTopic(attempts = "3") // after the 3rd failed attempt, the message is parked in topic TeleportFailed-dlt
    @KafkaListener(topics = "TeleportFailed", groupId = "notification")
    void onTeleportFailed(TeleportFailed event) {
        log.info("Attempt to notify");
        if (event.bookId() == 13) throw new IllegalStateException("SMTP down"); // TODO 2
        log.info("📧 Email/SMS to user {}: sorry, book {} could not be teleported", event.userId(), event.bookId());
    }
}
