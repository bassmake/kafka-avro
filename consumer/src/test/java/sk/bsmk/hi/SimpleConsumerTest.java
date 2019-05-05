package sk.bsmk.hi;

import com.salesforce.kafka.test.KafkaTestUtils;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import io.vavr.Tuple;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class SimpleConsumerTest {

  @RegisterExtension
  static final SharedKafkaTestResource kafka = new SharedKafkaTestResource();

  private final KafkaConsumerConfig config = ImmutableKafkaConsumerConfig.builder()
    .bootstrapServers(kafka.getKafkaConnectString())
    .id("test-consumer")
    .topic("consumer-test-topic")
    .groupId("test-consumer-group")
    .build();

  private final SimpleConsumer consumer = new SimpleConsumer(config);

  @Test
  void that_first_message_is_consumed() {
    final KafkaTestUtils utils = kafka.getKafkaTestUtils();
    utils.produceRecords(
      io.vavr.collection.HashMap.of("key-one", "message-one")
        .map((key, value) -> Tuple.of(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8)))
        .toJavaMap(),
      config.topic(),
      0
    );
    assertThat(utils.consumeAllRecordsFromTopic(config.topic())).hasSize(1);

    final ConsumerRecords<String, String> records = consumer.poll();
    assertThat(records).hasSize(1);
  }

}
