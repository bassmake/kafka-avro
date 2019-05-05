package sk.bsmk.hi;

import org.apache.kafka.common.serialization.Serde;

public interface KeyValueAvroSerde {

  Serde<Object> key();

  Serde<Object> value();
}
