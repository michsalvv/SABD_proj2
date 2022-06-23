package flink;

import kafka.exception.SimulationTimeException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import queries.exception.CoordinatesOutOfBoundException;
import queries.exception.TemperatureOutOfBoundException;

import java.nio.charset.StandardCharsets;

public class CustomDeserializer implements DeserializationSchema<Event> {

    @Override
    public Event deserialize(byte[] bytes) {
        Event event = new Event(new String(bytes, StandardCharsets.UTF_8));
        try{
            validateTemperature(event.getTemperature());
            validateCoordinates(event.getLatitude(),event.getLongitude());
            return event;
        } catch (TemperatureOutOfBoundException | CoordinatesOutOfBoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(Event nextElement) {
//        if (nextElement.getSensor_id() == 0){
//            System.out.println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
//            return true;
//        }
        return false;
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

    // LAT Y: 38 - 58
    // LON X: 2  - 30
    static void validateCoordinates(Double latitude, Double longitude) throws CoordinatesOutOfBoundException {
        if (latitude < 38 || latitude > 58 || longitude < 2 || latitude > 30) {
            throw new CoordinatesOutOfBoundException("Coordinate Error: Sensor out of grid");
        }
    }
}
