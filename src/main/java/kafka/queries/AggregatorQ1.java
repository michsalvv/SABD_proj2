package kafka.queries;

import org.apache.kafka.streams.kstream.Aggregator;
import scala.Tuple3;
import utils.tuples.ValQ1;

public class AggregatorQ1 implements Aggregator<Long, ValQ1, ValQ1> {


    @Override
    public ValQ1 apply(Long aLong, ValQ1 val_1, ValQ1 val_2) {
        Double temperature = val_1.getTemperature()+val_2.getTemperature();
        Long occurrences = val_1.getOccurrences()+val_2.getOccurrences();

        return new ValQ1(val_1.getTimestamp(), val_1.getSensor_id(), temperature, occurrences);
    }
}
