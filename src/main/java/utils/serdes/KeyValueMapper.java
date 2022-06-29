package utils.serdes;

import org.apache.kafka.streams.KeyValue;
import utils.tuples.ValQ2;

public class KeyValueMapper extends KeyValue<String, ValQ2> {
    public KeyValueMapper(String key, ValQ2 value) {
        super(key, value);
    }
}
