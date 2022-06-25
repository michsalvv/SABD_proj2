package flink.deserialize;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import flink.exception.CoordinatesOutOfBoundException;
import flink.exception.TemperatureOutOfBoundException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;

import java.nio.charset.StandardCharsets;

public class EventDeserializer implements DeserializationSchema<Event>, Deserializer<Event> {

    @Override
    public Event deserialize(byte[] bytes) {
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

    // Deserializzatore per Kafka
    @Override
    public Event deserialize(String topic, byte[] data) {
        Event event = new Event(new String(data, StandardCharsets.UTF_8));
        try{
            validateTemperature(event.getTemperature());
            return event;
        } catch (TemperatureOutOfBoundException e) {
//            e.printStackTrace();
            return null;
        }
    }
}
