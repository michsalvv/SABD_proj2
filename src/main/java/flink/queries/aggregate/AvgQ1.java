package flink.queries.aggregate;

import utils.Event;
import org.apache.flink.api.common.functions.AggregateFunction;
import utils.Tools;
import utils.tuples.OutputQuery;
import utils.tuples.ValQ1;

public class AvgQ1 implements AggregateFunction<Event, AccumulatorQ1, OutputQuery> {
    public AccumulatorQ1 createAccumulator() {
        return new AccumulatorQ1();
    }

    @Override
    public AccumulatorQ1 add(Event values, AccumulatorQ1 acc) {
        acc.sum += values.getTemperature();
        acc.count++;
        acc.sensor_id = values.getSensor_id();
        acc.last_timestamp = values.getTimestamp();
        return acc;
    }

    @Override
    public AccumulatorQ1 merge(AccumulatorQ1 a, AccumulatorQ1 b) {
        a.count += b.count;
        a.sum += b.sum;
        return a;
    }

    @Override
    public ValQ1 getResult(AccumulatorQ1 acc) {
        double mean = acc.sum / (double) acc.count;
        ValQ1 result = new ValQ1();
        result.setSensor_id(acc.sensor_id);
        result.setTemperature(mean);
        result.setOccurrences(acc.count);
        result.setTimestamp(Tools.getHourSlot(acc.last_timestamp));
        return result;
    }



}