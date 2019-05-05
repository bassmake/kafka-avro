package sk.bsmk.hi;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
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

  public SimpleProducer(KafkaProducerConfig config) {
    final Properties props = new Properties();
    props.put(ProducerConfig.CLIENT_ID_CONFIG, config.id());
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    this.producer = new KafkaProducer<>(props);
    this.topic = config.topic();
  }

  public Future<RecordMetadata> send(int number) {
    final String key = String.format("key-%d", number);
    final String message = String.format("message-%d", number);

    final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);
    return producer.send(record, (metadata, exception) -> log.info("After sending {}: {}", record, metadata));
  }

}
