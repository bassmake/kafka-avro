package sk.bsmk.hi;

import java.util.Properties;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroProducer {

  private static final Logger log = LoggerFactory.getLogger(AvroProducer.class);

  private final KafkaProducer<Object, Object> producer;
  private final String topic;

  public AvroProducer(KafkaProducerConfig config, KeyValueAvroSerde serde) {
    final Properties props = new Properties();
    props.put(ProducerConfig.CLIENT_ID_CONFIG, config.id());
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    producer = new KafkaProducer<>(props, serde.key().serializer(), serde.key().serializer());
    this.topic = config.topic();
  }

  public Future<RecordMetadata> send(TenantKey key, MonetaryTransaction transaction) {
    final ProducerRecord<Object, Object> record = new ProducerRecord<>(topic, key, transaction);
    return producer.send(
        record, (metadata, exception) -> log.info("After sending {}: {}", record, metadata));
  }
}
