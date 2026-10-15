package victor.kafka;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.http.MediaType.APPLICATION_JSON;

/** Needs all services up (see README). */
class CheckoutFailsIT {
    int userId = new Random().nextInt(1000, 1_000_000); // fresh user: no address yet
    RestClient rest = RestClient.create();
    TeleportFailedTopic teleportFailed = new TeleportFailedTopic();

    @AfterEach
    void closeConsumer() {
        teleportFailed.close();
    }

    @Test
    void teleportFailsWhenUserHasNoAddress() {
        Map<String, Object> checkoutRequest = Map.of(
                "userId", userId,
                "bookIds", List.of(13));
        rest.post().uri("http://localhost:8081/checkout") // accepted: the teleport happens later, async
                .contentType(APPLICATION_JSON).body(checkoutRequest).retrieve().toBodilessEntity();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(teleportFailed.eventsOf(userId)).containsExactly("{\"bookId\":13,\"userId\":" + userId + "}"));
    }
}
