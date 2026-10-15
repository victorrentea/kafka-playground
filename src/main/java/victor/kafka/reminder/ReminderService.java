package victor.kafka.reminder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import victor.kafka.Events.BookAssignedToUser;

@SpringBootApplication
public class ReminderService {
    private static final Logger log = LoggerFactory.getLogger(ReminderService.class);

    public static void main(String[] args) {
        SpringApplication.run(ReminderService.class, args);
    }

    @KafkaListener(topics = "BookAssignedToUser", groupId = "reminder") // TODO 1: reminder-${random.uuid}
    void onBookAssigned(BookAssignedToUser event) {
        log.info("⏰ Will remind user {} to return book {} in 14 days", event.userId(), event.bookId());
    }
}
