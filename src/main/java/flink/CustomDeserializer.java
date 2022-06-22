package flink;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.nio.charset.StandardCharsets;

public class CustomDeserializer implements DeserializationSchema<Event> {

    @Override
    public Event deserialize(byte[] bytes) {
        return new Event(new String(bytes, StandardCharsets.UTF_8));
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
}
