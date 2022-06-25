package flink.deserialize;


import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public class EventSerializer implements Serializer<Event> {
    @Override
    public byte[] serialize(String topic, Event data) {
        if (data == null)
            return null;
        return data.toString().getBytes(StandardCharsets.UTF_8);
    }


}
