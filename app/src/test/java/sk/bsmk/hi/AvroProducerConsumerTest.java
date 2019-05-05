package sk.bsmk.hi;

import static org.assertj.core.api.Assertions.assertThat;

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class AvroProducerConsumerTest {

  @RegisterExtension static final SharedKafkaTestResource kafka = new SharedKafkaTestResource();

  private final String topic = "test-avro-topic";
  private final KeyValueAvroSerde serde = new KeyValueAvroMockSchemaRegistrySerde();

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

  private final AvroProducer producer = new AvroProducer(producerConfig, serde);
  private final AvroConsumer consumer = new AvroConsumer(consumerConfig, serde);

  @Test
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
    assertThat(kafka.getKafkaTestUtils().consumeAllRecordsFromTopic(topic)).hasSize(1);

    final ConsumerRecords<Object, Object> records = consumer.poll();

    final ConsumerRecord<Object, Object> record = extractOne(records);
    final TenantKey receivedKey = (TenantKey) record.key();
    final MonetaryTransaction receivedTransaction = (MonetaryTransaction) record.value();

    assertThat(receivedKey).isEqualTo(key);
    assertThat(receivedTransaction).isEqualTo(transaction);
  }

  private ConsumerRecord<Object, Object> extractOne(ConsumerRecords<Object, Object> records) {
    assertThat(records).hasSize(1);
    for (ConsumerRecord<Object, Object> record : records) {
      return record;
    }
    throw new IllegalStateException("Expecting just one record");
  }
}
