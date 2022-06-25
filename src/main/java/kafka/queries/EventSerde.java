package kafka.queries;

import flink.deserialize.Event;
import flink.deserialize.EventDeserializer;
import flink.deserialize.EventSerializer;
import flink.exception.TemperatureOutOfBoundException;
import org.apache.kafka.common.serialization.*;

import java.nio.charset.StandardCharsets;

public class EventSerde {

    public EventSerde() {
    }

    public static Serde Event() {
        EventDeserializer deserializer = new EventDeserializer();
        EventSerializer serializer = new EventSerializer();
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
