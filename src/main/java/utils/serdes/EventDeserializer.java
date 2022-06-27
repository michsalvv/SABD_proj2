package utils.serdes;

import flink.exception.TemperatureOutOfBoundException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import utils.Event;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;

public class EventDeserializer implements Deserializer<Event>, DeserializationSchema<Event> {

    private Class<Event> destinationClass;
    private Type reflectionTypeToken;

    public EventDeserializer() { }

    public EventDeserializer(Class<Event> destinationClass) {
        this.destinationClass = destinationClass;
    }

    public EventDeserializer(Type reflectionTypeToken) {
        this.reflectionTypeToken = reflectionTypeToken;
    }

    @Override
    public Event deserialize(String topic, byte[] bytes) {
        Event event = new Event(new String(bytes, StandardCharsets.UTF_8));
        try{
            validateTemperature(event.getTemperature());
            return event;
        } catch (TemperatureOutOfBoundException e) {
//            e.printStackTrace();
            return null;
        }
    }

    // La temperatura massima rilevabile del sensore varia da -40° a +85°
    static void validateTemperature(Double temperature) throws TemperatureOutOfBoundException {
        if (temperature < -40 || temperature > 85) {
            throw new TemperatureOutOfBoundException("Deserializer Error: Temperature out of Sensor Range");
        }
    }

    @Override
    public Event deserialize(byte[] bytes) throws IOException {
        Event event = new Event(new String(bytes, StandardCharsets.UTF_8));
        try{
            validateTemperature(event.getTemperature());
            return event;
        } catch (TemperatureOutOfBoundException e) {
//            e.printStackTrace();
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(Event event) {
        return false;
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(Event.class);
    }
}
