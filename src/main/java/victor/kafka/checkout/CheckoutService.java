package victor.kafka.checkout;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpStatusCode;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClient;
import org.springframework.web.server.ResponseStatusException;
import victor.kafka.Events.TeleportBooksToUser;

import java.util.List;

@SpringBootApplication
@RestController
public class CheckoutService {
  public static void main(String[] args) {
    SpringApplication.run(CheckoutService.class, "--server.port=8081");
  }

  @Autowired KafkaTemplate<Integer, Object> kafka;
  RestClient rest = RestClient.create("http://localhost:8082");

  record CheckoutRequest(List<Integer> bookIds, int userId) {}

  @PostMapping("/checkout")
  void checkout(@RequestBody CheckoutRequest request) {
    // the max-5 check stays SYNC: the user waits for its answer
    rest.post().uri("/user/{u}/books", request.userId()).body(request.bookIds()).retrieve()
        .onStatus(HttpStatusCode::is4xxClientError, (req, res) -> { throw new ResponseStatusException(res.getStatusCode()); })
        .toBodilessEntity();
    kafka.send("TeleportBooksToUser", request.userId(), new TeleportBooksToUser(request.bookIds(), request.userId()));
  }
}
