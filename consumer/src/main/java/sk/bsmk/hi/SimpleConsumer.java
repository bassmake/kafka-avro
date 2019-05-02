package sk.bsmk.hi;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SimpleConsumer {

  private static final Logger log = LoggerFactory.getLogger(SimpleConsumer.class);
  private final Properties config;
  private final String topic;

  public SimpleConsumer(String bootstrapServers, String id, String groupId, String topic) {
    final Properties config = new Properties();
    config.put(ConsumerConfig.CLIENT_ID_CONFIG, id);
    config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    this.config = config;
    this.topic = topic;
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
