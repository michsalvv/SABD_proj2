package flink;

import kafka.exception.SimulationTimeException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import queries.exception.NegativeTemperatureException;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;

public class CustomDeserializer implements DeserializationSchema<Event> {

    @Override
    public Event deserialize(byte[] bytes) {
        Event event = new Event(new String(bytes, StandardCharsets.UTF_8));
        try{
            validateTemperature(event.getTemperature());
            return event;
        } catch (SimulationTimeException e) {
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

    static void validateTemperature(Double temperature) throws SimulationTimeException {
        if (temperature < -40 || temperature > 85) {
            throw new SimulationTimeException("Deserializer Error: Temperature out of Sensor Range");
        }
    }
}
