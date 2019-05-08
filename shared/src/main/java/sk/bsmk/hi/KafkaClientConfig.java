package sk.bsmk.hi;

import java.util.Properties;
import org.immutables.value.Value;

public interface KafkaClientConfig {

  String bootstrapServers();

  String id();

  String topic();

  @Value.Default
  default Properties additional() {
    return new Properties();
  }
}
