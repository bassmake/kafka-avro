package sk.bsmk.hi;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class KeyValueAvroMockSchemaRegistrySerde implements KeyValueAvroSerde {

  private final SchemaRegistryClient client = new MockSchemaRegistryClient();

  private final Serde<Object> key = serde(true);
  private final Serde<Object> value = serde(false);

  @Override
  public Serde<Object> key() {
    return key;
  }

  @Override
  public Serde<Object> value() {
    return value;
  }

  private Serde<Object> serde(boolean isKey) {
    return Serdes.serdeFrom(serializer(isKey), deserializer(isKey));
  }

  private KafkaAvroSerializer serializer(boolean isKey) {
    final Map<String, Object> props = new HashMap<>();
    props.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
    props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "unused");
    final KafkaAvroSerializer serializer = new KafkaAvroSerializer(client);
    serializer.configure(props, isKey);
    return serializer;
  }

  private KafkaAvroDeserializer deserializer(boolean isKey) {
    final Map<String, Object> props = new HashMap<>();
    props.put(KafkaAvroDeserializerConfig.AUTO_REGISTER_SCHEMAS, true);
    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
    props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "unused");
    final KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(client);
    deserializer.configure(props, isKey);
    return deserializer;
  }
}
