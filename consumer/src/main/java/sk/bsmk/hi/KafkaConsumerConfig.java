package sk.bsmk.hi;

import org.immutables.value.Value;

@Value.Immutable
@Value.Style(stagedBuilder = true)
public interface KafkaConsumerConfig extends KafkaClientConfig {

  String groupId();

}
