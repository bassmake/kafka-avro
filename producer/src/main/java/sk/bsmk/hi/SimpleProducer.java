package sk.bsmk.hi;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Future;

public class SimpleProducer {

  private static final Logger log = LoggerFactory.getLogger(SimpleProducer.class);

  private final KafkaProducer<String, String> producer;
  private final String topic;

  public SimpleProducer(String bootstrapServers, String id, String topic) {
    Properties config = new Properties();
    config.put("client.id", id);
    config.put("bootstrap.servers", bootstrapServers);
    config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    config.put("acks", "all");
    producer = new KafkaProducer<>(config);
    this.topic = topic;
  }

  public Future<RecordMetadata> send(int number) {
    final String key = String.format("key-%d", number);
    final String message = String.format("message-%d", number);

    final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);
    return producer.send(record, (metadata, exception) -> {
        log.info("After sending {}: {}", record, metadata);
    });
  }

}
