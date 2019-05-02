package sk.bsmk.hi;

import com.salesforce.kafka.test.KafkaTestUtils;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import io.vavr.Tuple;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class SimpleConsumerTest {

  @RegisterExtension
  static final SharedKafkaTestResource kafka = new SharedKafkaTestResource();

  private final String topic = "producer-test-topic";

  private final SimpleConsumer consumer = new SimpleConsumer(
    kafka.getKafkaConnectString(),
    "test-consumer",
    "test-group",
    topic
  );

  @BeforeEach
  void init_topic() {
    final KafkaTestUtils utils = kafka.getKafkaTestUtils();
    utils.produceRecords(
      io.vavr.collection.HashMap.of("key-one", "message-one")
        .map((key, value) -> Tuple.of(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8)))
        .toJavaMap(),
      topic,
      0
    );
  }

  @Test
  void that_first_message_is_consumed() {
    final KafkaTestUtils utils = kafka.getKafkaTestUtils();
    assertThat(utils.consumeAllRecordsFromTopic(topic)).hasSize(1);

    final ConsumerRecords<String, String> records = consumer.poll();
    assertThat(records).hasSize(1);
  }

}
