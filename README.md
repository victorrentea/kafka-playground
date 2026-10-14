# Kafka Playground — the 2150 teleporting library

Our library lends books, max 5 per user, teleported to their living room.

```
Browser ─POST /checkout→ Checkout ─POST /user/{u}/books (sync, max 5)→ User ─BookAssignedToUser→ Reminder
                            └─TeleportBooksToUser (async command)→ Teleport ×2 ←UserAddressUpdated─ User
                                                                   └─TeleportFailed→ Notification + User (undo)
```

**No Docker**: Kafka runs from its jar.

## Run (from the IDE, in this order)
1. `KafkaBroker`: Kafka on localhost:9092. `mvn clean` wipes its data.
2. `CheckoutService` (:8081), `UserService` (:8082), `ReminderService`, `NotificationService`
3. `TeleportService`, **twice** (allow multiple instances): each gets one partition of `TeleportBooksToUser`

## Play
```bash
# no address yet ⇒ TeleportFailed ⇒ notification + books removed from user
curl -H 'Content-Type: application/json' localhost:8081/checkout -d '{"userId":2,"bookIds":[1,6]}'
curl -X PUT -H 'Content-Type: text/plain' localhost:8082/user/2/address -d 'Living room, 2150'
curl -H 'Content-Type: application/json' localhost:8081/checkout -d '{"userId":2,"bookIds":[3,4,5,7,8]}'
curl -H 'Content-Type: application/json' localhost:8081/checkout -d '{"userId":2,"bookIds":[9]}'  # 409: max 5
# user 1 hashes to the other partition ⇒ handled by the other Teleport instance
curl -X PUT -H 'Content-Type: text/plain' localhost:8082/user/1/address -d 'Moon base'
curl -H 'Content-Type: application/json' localhost:8081/checkout -d '{"userId":1,"bookIds":[10]}'
```
