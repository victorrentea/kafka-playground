package victor.kafka;

import java.util.List;

/** One topic per message, named after the record. Key = userId ⇒ per-user ordering. */
public interface Events {
    record TeleportBooksToUser(List<Integer> bookIds, int userId) {} // async command
    record BookAssignedToUser(int bookId, int userId) {}
    record UserAddressUpdated(int userId, String address) {}
    record TeleportFailed(int bookId, int userId) {}
}
