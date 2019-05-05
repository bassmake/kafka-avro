package sk.bsmk.hi;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroConsumer {

  private static final Logger log = LoggerFactory.getLogger(AvroConsumer.class);
  private final Properties config;
  private final String topic;

  public AvroConsumer(KafkaConsumerConfig config) {
    final Properties props = new Properties();
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, config.id());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, config.groupId());
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    this.config = props;
    this.topic = config.topic();
  }

  public ConsumerRecords<Object, Object> poll() {
    try (KafkaConsumer<Object, Object> consumer =
        new KafkaConsumer<>(
            config,
            AvroMockRegistrySerDe.serde(true).deserializer(),
            AvroMockRegistrySerDe.serde(false).deserializer())) {
      consumer.subscribe(Collections.singleton(topic));
      final ConsumerRecords<Object, Object> records = consumer.poll(Duration.ofSeconds(10));
      consumer.commitSync();
      log.info("Polled {} records", records.count());
      return records;
    }
  }
}
