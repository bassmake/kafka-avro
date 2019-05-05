package sk.bsmk.hi;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

public class AvroProducer {

  private static final Logger log = LoggerFactory.getLogger(AvroProducer.class);

  private final KafkaProducer<Object, Object> producer;
  private final String topic;

  public AvroProducer(
    KafkaProducerConfig config
  ) {
    final Properties props = new Properties();
    props.put(ProducerConfig.CLIENT_ID_CONFIG, config.id());
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
//    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
//    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    producer = new KafkaProducer<>(props, serializer(true), serializer(false));
    this.topic = config.topic();
  }

  public Future<RecordMetadata> send(TenantKey key, MonetaryTransaction transaction) {
    final ProducerRecord<Object, Object> record = new ProducerRecord<>(topic, key, transaction);
    return producer.send(record, (metadata, exception) -> log.info("After sending {}: {}", record, metadata));
  }

  private KafkaAvroSerializer serializer(boolean isKey) {
    final Map<String, Object> serializerProps = new HashMap<>();
    serializerProps.put(KafkaAvroDeserializerConfig.AUTO_REGISTER_SCHEMAS, true);
    serializerProps.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "unused");
    final SchemaRegistryClient client = new MockSchemaRegistryClient();
    final KafkaAvroSerializer serializer = new KafkaAvroSerializer(client);
    serializer.configure(serializerProps, isKey);
    return serializer;
  }

}
