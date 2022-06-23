package queries.flink.aggregate;

import flink.Event;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import utils.ValQ2;

import java.sql.Timestamp;
import java.util.Calendar;

public class Average2 implements AggregateFunction<Event, AverageAccumulator2, ValQ2> {
    public AverageAccumulator2 createAccumulator() {
        return new AverageAccumulator2();
    }

    @Override
    public AverageAccumulator2 add(Event values, AverageAccumulator2 acc) {
        acc.sum += values.getTemperature();
        acc.count++;
        acc.location = values.getLocation();
        acc.last_timestamp = values.getTimestamp();
        return acc;
    }

    @Override
    public AverageAccumulator2 merge(AverageAccumulator2 a, AverageAccumulator2 b) {
        a.count += b.count;
        a.sum += b.sum;
        return a;
    }

    @Override
    public ValQ2 getResult(AverageAccumulator2 acc) {
        double mean = acc.sum / (double) acc.count;
        ValQ2 result = new ValQ2();
        result.setLocation(acc.location);
        result.setTemperature(mean);
        result.setOccurrences(acc.count);
        result.setTimestamp(utils.Tools.getSecondsSlot(acc.last_timestamp,5));
        return result;
    }
}