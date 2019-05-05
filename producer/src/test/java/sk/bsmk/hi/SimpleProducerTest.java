package sk.bsmk.hi;

import com.salesforce.kafka.test.KafkaTestUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SimpleProducerTest extends KafkaProducerTest {

  private final SimpleProducer producer = new SimpleProducer(config);

  @Test
  void that_first_message_is_produced() throws Exception {
    producer.send(1).get();

    final KafkaTestUtils utils = kafka.getKafkaTestUtils();
    final List<ConsumerRecord<byte[], byte[]>> records = utils.consumeAllRecordsFromTopic(config.topic());
    assertThat(records).hasSize(1);
  }

}
