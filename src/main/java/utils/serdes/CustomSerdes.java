package utils.serdes;

import kafka.queries.LocationAggregator;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import utils.Event;
import utils.tuples.ValQ1;
import utils.tuples.ValQ2;

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

    public static Serde<ValQ2> ValQ2(){
        ValQ2Deserializer deserializer = new ValQ2Deserializer(ValQ2.class);
        ValQ2Serializer serializer = new ValQ2Serializer();
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<LocationAggregator> LocationAggregator(){
        LocationAggregatorSerde serde = new LocationAggregatorSerde();
        return Serdes.serdeFrom(serde, serde);

    }
}
