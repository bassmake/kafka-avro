package sk.bsmk.hi;

import com.salesforce.kafka.test.KafkaTestUtils;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

class SimpleProducerTest {

  @RegisterExtension
  static final SharedKafkaTestResource kafka = new SharedKafkaTestResource();

  private static String topic = "producer-test-topic";

  private final SimpleProducer producer = new SimpleProducer(
    kafka.getKafkaConnectString(),
    "test-producer",
    topic
  );

  @Test
  void that_first_message_is_produced() throws Exception {
    final KafkaTestUtils utils = kafka.getKafkaTestUtils();
    producer.send(1).get();

    final List<ConsumerRecord<byte[], byte[]>> records = utils.consumeAllRecordsFromTopic(topic);
    assertThat(records).hasSize(1);
  }

}
