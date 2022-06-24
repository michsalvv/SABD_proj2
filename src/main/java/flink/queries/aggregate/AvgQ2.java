package flink.queries.aggregate;

import flink.deserialize.Event;
import org.apache.flink.api.common.functions.AggregateFunction;
import utils.Tools;
import utils.tuples.ValQ2;

public class AvgQ2 implements AggregateFunction<Event, AccumulatorQ2, ValQ2> {
    public AccumulatorQ2 createAccumulator() {
        return new AccumulatorQ2();
    }

    @Override
    public AccumulatorQ2 add(Event values, AccumulatorQ2 acc) {
        acc.sum += values.getTemperature();
        acc.count++;
        acc.location = values.getLocation();
        acc.last_timestamp = values.getTimestamp();
        return acc;
    }

    @Override
    public AccumulatorQ2 merge(AccumulatorQ2 a, AccumulatorQ2 b) {
        a.count += b.count;
        a.sum += b.sum;
        return a;
    }

    @Override
    public ValQ2 getResult(AccumulatorQ2 acc) {
        double mean = acc.sum / (double) acc.count;
        ValQ2 result = new ValQ2();
        result.setLocation(acc.location);
        result.setMeanTemperature(mean);
        result.setOccurrences(acc.count);
        result.setTimestamp(Tools.getHourSlot(acc.last_timestamp));
        return result;
    }
}