package victor.kafka;

import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.http.MediaType.TEXT_PLAIN;

/** Needs all services up (see README). */
class CheckoutSucceedsIT {
    int userId = new Random().nextInt(1000, 1_000_000); // fresh user: no books left over from previous runs
    RestClient rest = RestClient.create();

    @Test
    void bookIsTeleportedToTheUserAddress() throws InterruptedException {
        rest.put().uri("http://localhost:8082/user/{u}/address", userId)
                .contentType(TEXT_PLAIN).body("Living room, 2150").retrieve().toBodilessEntity();
        Thread.sleep(500); // address and command travel on different topics ⇒ let Teleport learn the address first

        Map<String, Object> checkoutRequest = Map.of("userId", userId, "bookIds", List.of(1));
        rest.post().uri("http://localhost:8081/checkout") // throws on 4xx/5xx
                .contentType(APPLICATION_JSON).body(checkoutRequest).retrieve().toBodilessEntity();

        assertThat(TeleportFailedTopic.waitForEventOf(userId, Duration.ofSeconds(5))).isEmpty();
    }
}
