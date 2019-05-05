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

  private final KafkaProducerConfig config = ImmutableKafkaProducerConfig.builder()
    .bootstrapServers(kafka.getKafkaConnectString())
    .id("test-producer")
    .topic("producer-test-topic")
    .build();

  private final SimpleProducer producer = new SimpleProducer(config);

  @Test
  void that_first_message_is_produced() throws Exception {
    final KafkaTestUtils utils = kafka.getKafkaTestUtils();
    producer.send(1).get();

    final List<ConsumerRecord<byte[], byte[]>> records = utils.consumeAllRecordsFromTopic(config.topic());
    assertThat(records).hasSize(1);
  }

}
