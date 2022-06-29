package utils.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import utils.tuples.ValQ1;
import utils.tuples.ValQ2;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;

public class ValQ2Deserializer implements Deserializer<ValQ2> {

    private Class<ValQ2> destinationClass;
    private Type reflectionTypeToken;

    public ValQ2Deserializer() { }

    public ValQ2Deserializer(Class<ValQ2> destinationClass) {
        this.destinationClass = destinationClass;
    }

    public ValQ2Deserializer(Type reflectionTypeToken) {
        this.reflectionTypeToken = reflectionTypeToken;
    }

    @Override
    public ValQ2 deserialize(String topic, byte[] bytes) {
        return new ValQ2(new String(bytes, StandardCharsets.UTF_8));
    }
}
