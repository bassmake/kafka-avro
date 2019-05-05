package sk.bsmk.hi;

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import org.junit.jupiter.api.extension.RegisterExtension;

public class KafkaProducerTest {

  @RegisterExtension
  static final SharedKafkaTestResource kafka = new SharedKafkaTestResource();

  final KafkaProducerConfig config = ImmutableKafkaProducerConfig.builder()
    .bootstrapServers(kafka.getKafkaConnectString())
    .id("test-producer")
    .topic("producer-test-topic")
    .build();

}
