package sk.bsmk.hi;

import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(stagedBuilder = true)
public interface KafkaProducerConfig extends KafkaClientConfig {

  @Value.Derived
  default Properties properties() {
    final Properties props = additional();
    props.put(ProducerConfig.CLIENT_ID_CONFIG, id());
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    return props;
  }
}
