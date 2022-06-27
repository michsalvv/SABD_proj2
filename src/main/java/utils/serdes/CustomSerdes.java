package utils.serdes;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import utils.Event;

public final class CustomSerdes {

    public CustomSerdes() {}

    public static Serde<Event> Event() {
        EventSerializer serializer = new EventSerializer();
        EventDeserializer deserializer = new EventDeserializer(Event.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
