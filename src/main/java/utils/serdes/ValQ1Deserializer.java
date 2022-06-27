package utils.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import utils.tuples.ValQ1;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;

public class ValQ1Deserializer implements Deserializer<ValQ1> {

    private Class<ValQ1> destinationClass;
    private Type reflectionTypeToken;

    public ValQ1Deserializer() { }

    public ValQ1Deserializer(Class<ValQ1> destinationClass) {
        this.destinationClass = destinationClass;
    }

    public ValQ1Deserializer(Type reflectionTypeToken) {
        this.reflectionTypeToken = reflectionTypeToken;
    }

    @Override
    public ValQ1 deserialize(String topic, byte[] bytes) {
        return new ValQ1(new String(bytes, StandardCharsets.UTF_8));
    }
}
