package flink.deserialize;

import flink.exception.CoordinatesOutOfBoundException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import flink.exception.TemperatureOutOfBoundException;

import java.nio.charset.StandardCharsets;

public class EventDeserializer implements DeserializationSchema<Event> {

    @Override
    public Event deserialize(byte[] bytes) {
        Event event = new Event(new String(bytes, StandardCharsets.UTF_8));
        try{
            validateTemperature(event.getTemperature());
            validateCoordinates(event.getLatitude(),event.getLongitude());
            return event;
        } catch (TemperatureOutOfBoundException | CoordinatesOutOfBoundException e) {
//            e.printStackTrace();
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(Event nextElement) {
        if (nextElement.getSensor_id() == -1){
            System.out.println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
            return true;
        }
        return true;
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(Event.class);
    }

    // La temperatura massima rilevabile del sensore varia da -40° a +85°
    static void validateTemperature(Double temperature) throws TemperatureOutOfBoundException {
        if (temperature < -40 || temperature > 85) {
            throw new TemperatureOutOfBoundException("Deserializer Error: Temperature out of Sensor Range");
        }
    }

    // La temperatura massima rilevabile del sensore varia da -40° a +85°
    static void validateCoordinates(Double latitude, Double longitude) throws CoordinatesOutOfBoundException {
        if (latitude < -90D || latitude > 90D || longitude < -180D || longitude > 180D) {
            throw new CoordinatesOutOfBoundException("Deserializer Error: Coordinates out of Bound");
        }
    }
}
