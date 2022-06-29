package utils.serdes;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import utils.tuples.Event;
import utils.tuples.ValQ1;
import utils.tuples.ValQ2;

public final class CustomSerdes {

    public CustomSerdes() {}

    public static Serde<Event> Event() {
        EventSerde serdes = new EventSerde();
        return Serdes.serdeFrom(serdes, serdes);
    }

    public static Serde<ValQ1> ValQ1() {
        ValQ1Serde serdes = new ValQ1Serde();
        return Serdes.serdeFrom(serdes, serdes);
    }

    public static Serde<ValQ2> ValQ2() {
        ValQ2Serde serdes = new ValQ2Serde();
        return Serdes.serdeFrom(serdes, serdes);

    }
}
