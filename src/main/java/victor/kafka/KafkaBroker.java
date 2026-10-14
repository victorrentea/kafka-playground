package victor.kafka;

import kafka.Kafka;
import kafka.tools.StorageTool;

/** Starts a real Kafka broker from the jar on the classpath (no Docker, no download). */
public class KafkaBroker {
  public static void main(String[] args) {
    StorageTool.execute(new String[]{"format", "--standalone", "--ignore-formatted",
        "-t", "MkU3OEVBNTcwNTJENDM2Qk", "-c", "kafka.properties"}, System.out); // fixed cluster id ⇒ restartable
    Kafka.main(new String[]{"kafka.properties"});
  }
}
