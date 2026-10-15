package victor.kafka.user;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import victor.kafka.Events.BookAssignedToUser;
import victor.kafka.Events.TeleportFailed;
import victor.kafka.Events.UserAddressUpdated;

import java.util.*;

import static org.springframework.http.HttpStatus.CONFLICT;

@SpringBootApplication
@RestController
public class UserService {
    public static void main(String[] args) {
        SpringApplication.run(UserService.class, "--server.port=8082");
    }

    @Autowired KafkaTemplate<String, Object> kafka;
    Map<Integer, Set<Integer>> booksOfUser = new HashMap<>(); // fake DB

    @PostMapping("/user/{userId}/books")
    synchronized void addBooks(@PathVariable int userId, @RequestBody List<Integer> bookIds) {
        Set<Integer> existingBooks = booksOfUser.computeIfAbsent(userId, k -> new HashSet<>());
        if (existingBooks.size() + bookIds.size() > 5) throw new ResponseStatusException(CONFLICT, "Max 5 books/user");
        existingBooks.addAll(bookIds);
        for (int bookId : bookIds) {
            kafka.send("BookAssignedToUser", String.valueOf(userId), new BookAssignedToUser(bookId, userId));
        }
    }

    @PutMapping("/user/{userId}/address")
    void updateAddress(@PathVariable int userId, @RequestBody String address) {
        kafka.send("UserAddressUpdated", String.valueOf(userId), new UserAddressUpdated(userId, address));
    }

    @KafkaListener(topics = "TeleportFailed", groupId = "user")
    synchronized void compensate(TeleportFailed event) {
        booksOfUser.getOrDefault(event.userId(), new HashSet<>()).remove(event.bookId());
    }
}
