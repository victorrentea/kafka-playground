package victor.kafka.notification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import victor.kafka.Events.TeleportFailed;

@SpringBootApplication
public class NotificationService {
    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    public static void main(String[] args) {
        SpringApplication.run(NotificationService.class, args);
    }

    @KafkaListener(topics = "TeleportFailed", groupId = "notification")
    void onTeleportFailed(TeleportFailed event) {
//        if (event.bookId() == 6) throw new IllegalStateException("SMTP down"); // TODO 2
        log.info("📧 Email/SMS to user {}: sorry, book {} could not be teleported", event.userId(), event.bookId());
    }
}
