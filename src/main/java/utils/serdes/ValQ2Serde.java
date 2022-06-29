package utils.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import utils.tuples.ValQ2;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ValQ2Serde implements Deserializer<ValQ2>, Serializer<ValQ2> {

    public ValQ2Serde() { }

    @Override
    public ValQ2 deserialize(String topic, byte[] bytes) {
        return new ValQ2(new String(bytes, StandardCharsets.UTF_8));
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String s, ValQ2 values) {
        if (values == null) {
            System.out.println("Null receiving at serializing");
            return null;
        }
        return values.toSerialize().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        Serializer.super.close();
    }
}
