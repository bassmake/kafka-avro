package sk.bsmk.hi;

public interface KafkaClientConfig {

  String bootstrapServers();

  String id();

  String topic();
}
