package victor.kafka.reminder;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import victor.kafka.Events.BookAssignedToUser;

@SpringBootApplication
public class ReminderService {
  public static void main(String[] args) {
    SpringApplication.run(ReminderService.class, args);
  }

  @KafkaListener(topics = "BookAssignedToUser", groupId = "reminder")
  void onBookAssigned(BookAssignedToUser event) {
    System.out.println("⏰ Will remind user " + event.userId() + " to return book " + event.bookId() + " in 14 days");
  }
}
