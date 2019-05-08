package sk.bsmk.hi;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(stagedBuilder = true)
public interface KafkaConsumerConfig extends KafkaClientConfig {

  String groupId();

  @Value.Derived
  default Properties properties() {
    final Properties props = additional();
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, id());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId());
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return props;
  }
}
