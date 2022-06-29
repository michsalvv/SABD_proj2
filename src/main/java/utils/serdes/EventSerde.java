package utils.serdes;

import utils.exception.CoordinatesOutOfBoundException;
import utils.exception.TemperatureOutOfBoundException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.kafka.common.serialization.Serializer;
import utils.tuples.Event;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class EventSerde implements Deserializer<Event>, DeserializationSchema<Event>, Serializer<Event> {

    public EventSerde() { }

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

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String s, Event event) {
        if (event == null) {
            System.out.println("Null receiving at serializing");
            return null;
        }
        return event.toString_reduced().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        Serializer.super.close();
    }
}
