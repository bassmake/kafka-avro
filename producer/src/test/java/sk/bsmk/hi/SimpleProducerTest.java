package sk.bsmk.hi;

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import static org.junit.jupiter.api.Assertions.fail;

public class KafkaProducerTest {

  @RegisterExtension
  public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource();

  @Test
  void just() {
    fail("Implement me!!!");
  }

}
