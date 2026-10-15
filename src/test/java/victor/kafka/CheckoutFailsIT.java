package victor.kafka;

import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.http.MediaType.APPLICATION_JSON;

/** Needs all services up (see README). */
class CheckoutFailsIT {
    int userId = new Random().nextInt(1000, 1_000_000); // fresh user: no address yet
    RestClient rest = RestClient.create();

    @Test
    void teleportFailsWhenUserHasNoAddress() {
        Map<String, Object> checkoutRequest = Map.of("userId", userId, "bookIds", List.of(1));
        rest.post().uri("http://localhost:8081/checkout") // accepted: the teleport happens later, async
                .contentType(APPLICATION_JSON).body(checkoutRequest).retrieve().toBodilessEntity();

        assertThat(TeleportFailedTopic.waitForEventOf(userId, Duration.ofSeconds(10)))
                .hasValue("{\"bookId\":1,\"userId\":" + userId + "}");
    }
}
