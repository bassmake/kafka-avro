package sk.bsmk.hi;

import com.salesforce.kafka.test.KafkaTestUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class AvroProducerTest extends KafkaProducerTest {

  private final AvroProducer producer = new AvroProducer(config);

  @Test
  void that_avro_message_is_produced() throws Exception {
    producer.send(
      TenantKey.newBuilder()
        .setTenant("test-one")
        .build(),
      MonetaryTransaction.newBuilder()
        .setId(UUID.randomUUID().toString())
        .setSource("TEST")
        .setAmount(23.43)
        .setCurrency("EUR")
        .build()
      ).get();

    final KafkaTestUtils utils = kafka.getKafkaTestUtils();
    final List<ConsumerRecord<byte[], byte[]>> records = utils.consumeAllRecordsFromTopic(config.topic());
    assertThat(records).hasSize(1);
  }

}
