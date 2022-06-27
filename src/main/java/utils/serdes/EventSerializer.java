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
        int size_timestamp;
        int size_sensor_id;
        int size_temperature;
        int size_location;
        int size_latitude;
        int size_longitude;

        //byte[] timestamp;
        //byte[] sensor_id;
        //byte[] temperature;
        //byte[] location;
        //byte[] latitude;
        //byte[] longitude;

            if (event == null) {
                System.out.println("Null receiving at serializing");
                return null;
            }
            try {
            /*
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            ObjectMapper mapper = new ObjectMapper();
            mapper.setDateFormat(df);
            timestamp = mapper.writeValueAsString(event).getBytes(encoding);
            size_timestamp = timestamp.length;

            sensor_id = event.getSensor_id().toString().getBytes(encoding);
            size_sensor_id = sensor_id.length;

            temperature = event.getTemperature().toString().getBytes(encoding);
            size_temperature = temperature.length;

            location = event.getLocation().toString().getBytes(encoding);
            size_location = location.length;

            latitude = event.getLatitude().toString().getBytes(encoding);
            size_latitude = latitude.length;

            longitude = event.getLongitude().toString().getBytes(encoding);
            size_longitude = longitude.length;

            ByteBuffer buffer = ByteBuffer.allocate(4+size_timestamp + 8+size_sensor_id +
                    8+size_temperature + 8+size_location + 8+size_latitude + 8+size_longitude);

            buffer.putInt(size_timestamp);
            buffer.put(timestamp);
            buffer.putInt(size_sensor_id);
            buffer.put(sensor_id);
            buffer.putInt(size_temperature);
            buffer.put(temperature);
            buffer.putInt(size_location);
            buffer.put(location);
            buffer.putInt(size_latitude);
            buffer.put(latitude);
            buffer.putInt(size_longitude);
            buffer.put(longitude);
            System.out.println("serializzazione: " + buffer.array());
            return buffer.array();
*/
            //return event.toString().getBytes(StandardCharsets.UTF_8);
            //EventDeserializer des = new EventDeserializer();
            System.out.println("serializing...");
            Timestamp timestamp = event.getTimestamp();
            Long sensor_id = event.getSensor_id();
            Double temperature = event.getTemperature();
            Long location = event.getLocation();
            Double latitude = event.getLatitude();
            Double longitude = event.getLongitude();
            var message = String.format("%s;%d;%,.4f;%d;%,.4f;%,.4f;", timestamp, sensor_id, temperature,
                    location, latitude, longitude);
            //System.out.println(des.deserialize(message.getBytes(StandardCharsets.UTF_8)));
            return message.getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new SerializationException("Error when serializing Event to byte[]");
        }
    }

    @Override
    public void close() {
        Serializer.super.close();
    }
}
