package victor.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import kafka.Kafka;
import kafka.tools.StorageTool;

/** Starts a real Kafka broker from the jar on the classpath (no Docker, no download). */
public class KafkaBroker {
    private static final Logger log = LoggerFactory.getLogger(KafkaBroker.class);

    public static void main(String[] a) {
        String[] args = {
                "format",
                "--standalone",
                "--ignore-formatted", // already formatted dir ⇒ skip, not fail ⇒ restartable
                "-t", "MkU3OEVBNTcwNTJENDM2Qk", // fixed cluster id, the same on every run
                "-c", "kafka.properties"};
        log.info("To wipe all broker data (topics, messages, offsets): stop me and delete target/ (mvn clean)");
        StorageTool.execute(args, System.out);
        Kafka.main(new String[]{"kafka.properties"});
    }
}
