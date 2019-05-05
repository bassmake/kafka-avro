package sk.bsmk.hi;

import static org.assertj.core.api.Assertions.assertThat;

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class AvroProducerConsumerTest {

  @RegisterExtension static final SharedKafkaTestResource kafka = new SharedKafkaTestResource();

  private final String topic = "test-avro-topic";

  private final KafkaProducerConfig producerConfig =
      ImmutableKafkaProducerConfig.builder()
          .bootstrapServers(kafka.getKafkaConnectString())
          .id("test-avro-producer")
          .topic(topic)
          .build();

  private final KafkaConsumerConfig consumerConfig =
      ImmutableKafkaConsumerConfig.builder()
          .bootstrapServers(kafka.getKafkaConnectString())
          .id("test-avro-consumer")
          .topic(topic)
          .groupId("test-consumer-group")
          .build();

  private final AvroProducer producer = new AvroProducer(producerConfig);
  private final AvroConsumer consumer = new AvroConsumer(consumerConfig);

  @Test
  @Disabled
  void that_one_message_is_produced_and_consumed() throws Exception {

    final TenantKey key = TenantKey.newBuilder().setTenant("tenant-" + UUID.randomUUID()).build();

    final MonetaryTransaction transaction =
        MonetaryTransaction.newBuilder()
            .setId(UUID.randomUUID().toString())
            .setSource("APP_TEST")
            .setAmount(42.43)
            .setCurrency("EUR")
            .build();

    final RecordMetadata data = producer.send(key, transaction).get();
    assertThat(data.offset()).isEqualTo(0L);

    final ConsumerRecords<Object, Object> records = consumer.poll();
    assertThat(records).hasSize(1);
  }
}
