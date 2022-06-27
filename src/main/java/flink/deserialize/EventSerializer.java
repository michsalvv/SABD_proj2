package flink.deserialize;


import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public class EventSerializer implements Serializer<Event> {
    @Override
    public byte[] serialize(String topic, Event data) {
        System.out.println("TO SERIALIZE: "+data.toString_reduced());
        if (data == null) {
            System.out.println("NULL");
            return null;
        }
        return data.toString_reduced().getBytes(StandardCharsets.UTF_8);
    }


}
