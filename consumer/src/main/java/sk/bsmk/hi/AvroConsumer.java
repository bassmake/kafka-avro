package sk.bsmk.hi;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroConsumer {

  private static final Logger log = LoggerFactory.getLogger(AvroConsumer.class);
  private final Properties props;
  private final String topic;
  private final KeyValueAvroSerde serde;

  public AvroConsumer(KafkaConsumerConfig config, KeyValueAvroSerde serde) {
    this.props = config.properties();
    this.serde = serde;
    this.topic = config.topic();
  }

  public ConsumerRecords<Object, Object> poll() {
    try (KafkaConsumer<Object, Object> consumer =
        new KafkaConsumer<>(props, serde.key().deserializer(), serde.value().deserializer())) {

      consumer.subscribe(Collections.singleton(topic));
      final ConsumerRecords<Object, Object> records = consumer.poll(Duration.ofSeconds(10));
      consumer.commitSync();
      log.info("Polled {} records", records.count());
      return records;
    }
  }
}
