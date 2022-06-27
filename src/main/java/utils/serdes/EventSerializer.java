package utils.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import utils.Event;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Map;

public class EventSerializer implements Serializer<Event> {
    private String encoding = "UTF8";
    private final ObjectMapper objectMapper = new ObjectMapper();
    public EventSerializer() { }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String s, Event event) {
        System.out.println("To serialize: " + event);
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
