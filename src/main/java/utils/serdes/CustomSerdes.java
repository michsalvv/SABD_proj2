package utils.serdes;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import utils.Event;
import utils.tuples.ValQ1;

public final class CustomSerdes {

    public CustomSerdes() {}

    public static Serde<Event> Event() {
        EventSerializer serializer = new EventSerializer();
        EventDeserializer deserializer = new EventDeserializer(Event.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<ValQ1> ValQ1() {
        ValQ1Serializer serializer = new ValQ1Serializer();
        ValQ1Deserializer deserializer = new ValQ1Deserializer(ValQ1.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
