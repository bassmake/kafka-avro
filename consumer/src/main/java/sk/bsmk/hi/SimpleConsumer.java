package sk.bsmk.hi;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleConsumer {

  private static final Logger log = LoggerFactory.getLogger(SimpleConsumer.class);
  private final Properties config;
  private final String topic;

  public SimpleConsumer(KafkaConsumerConfig config) {
    final Properties props = new Properties();
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, config.id());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, config.groupId());
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    this.config = props;
    this.topic = config.topic();
  }

  public ConsumerRecords<String, String> poll() {
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
      consumer.subscribe(Collections.singleton(topic));
      final ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
      consumer.commitSync();
      log.info("Polled {} records", records.count());
      return records;
    }
  }
}
