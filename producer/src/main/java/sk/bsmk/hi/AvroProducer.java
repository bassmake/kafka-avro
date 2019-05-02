package sk.bsmk.hi;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class AvroProducer {

  private static final Logger log = LoggerFactory.getLogger(AvroProducer.class);

  private final KafkaProducer<String, String> producer;
  private final String topic;

  public AvroProducer(String bootstrapServers, String id, String topic) {
    Properties config = new Properties();
    config.put(ProducerConfig.CLIENT_ID_CONFIG, id);
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
    config.put(ProducerConfig.ACKS_CONFIG, "all");
    producer = new KafkaProducer<>(config);
    this.topic = topic;
  }

}
