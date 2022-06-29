package utils.serdes;

import flink.exception.CoordinatesOutOfBoundException;
import flink.exception.TemperatureOutOfBoundException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.spark.sql.util.NumericHistogram;
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

    @Override //kafka-deserialize
    public Event deserialize(String topic, byte[] bytes) {
        Event event = new Event(new String(bytes, StandardCharsets.UTF_8));
        try{
            validateTemperature(event.getTemperature());
            validateCoordinates(event.getLatitude(), event.getLongitude());
            return event;
        } catch (TemperatureOutOfBoundException | CoordinatesOutOfBoundException e) {
//            e.printStackTrace();
            return null;
        }
    }

    @Override   //flink-deserialize
    public Event deserialize(byte[] bytes) {
        Event event = new Event(new String(bytes, StandardCharsets.UTF_8));
        try{
            validateTemperature(event.getTemperature());
            validateCoordinates(event.getLatitude(), event.getLongitude());
            return event;
        } catch (TemperatureOutOfBoundException | CoordinatesOutOfBoundException e) {
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

    // La temperatura massima rilevabile del sensore varia da -40° a +85°
    public static void validateTemperature(Double temperature) throws TemperatureOutOfBoundException {
        if (temperature < -40 || temperature > 85) {
            throw new TemperatureOutOfBoundException("Deserializer Error: Temperature out of Sensor Range");
        }
    }

    // La latitudine e longitudine variano da -90° a +90° e da -180° a +180*
    static void validateCoordinates(Double latitude, Double longitude) throws CoordinatesOutOfBoundException {
        if (latitude < -90D || latitude > 90D || longitude < -180D || longitude > 180D) {
            throw new CoordinatesOutOfBoundException("Deserializer Error: Coordinates out of Bound");
        }
    }
}
