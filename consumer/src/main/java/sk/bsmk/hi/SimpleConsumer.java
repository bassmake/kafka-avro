package sk.bsmk.hi;

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
    config.put("client.id", id);
    config.put("group.id", groupId);
    config.put("bootstrap.servers", bootstrapServers);
    config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
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
