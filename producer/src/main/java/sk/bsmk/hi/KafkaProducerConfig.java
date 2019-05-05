package sk.bsmk.hi;

import org.immutables.value.Value;

@Value.Immutable
@Value.Style(stagedBuilder = true)
public interface KafkaProducerConfig extends KafkaClientConfig {
}
