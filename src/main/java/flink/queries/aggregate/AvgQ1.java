package flink.queries.aggregate;

import flink.deserialize.Event;
import org.apache.flink.api.common.functions.AggregateFunction;
import utils.Config;
import utils.Tools;
import utils.tuples.OutputQuery;
import utils.tuples.OutQ1;

public class AvgQ1 implements AggregateFunction<Event, AccumulatorQ1, OutputQuery> {
    String windowType;
    public AvgQ1(String windowType) {
        this.windowType = windowType;
    }

    public AccumulatorQ1 createAccumulator() {
        return new AccumulatorQ1();
    }

    @Override
    public AccumulatorQ1 add(Event values, AccumulatorQ1 acc) {
        acc.sum += values.getTemperature();
        acc.sensor_id = values.getSensor_id();
        acc.last_timestamp = values.getTimestamp();
        acc.count++;
        return acc;
    }

    @Override
    public AccumulatorQ1 merge(AccumulatorQ1 a, AccumulatorQ1 b) {
        a.count += b.count;
        a.sum += b.sum;
        return a;
    }

    @Override
    public OutQ1 getResult(AccumulatorQ1 acc) {
        double mean = acc.sum / (double) acc.count;
        OutQ1 result = new OutQ1(windowType);
        result.setSensor_id(acc.sensor_id);
        result.setTemperature(mean);
        result.setOccurrences(acc.count);
        if (windowType.equals(Config.HOUR)) {
            result.setTimestamp(Tools.getHourSlot(acc.last_timestamp));
        }
        if (windowType.equals(Config.WEEK)) {
            result.setTimestamp(Tools.getWeekSlot(acc.last_timestamp));
        }
        if (windowType.equals(Config.MONTH)) {
            result.setTimestamp(Tools.getMonthSlot(acc.last_timestamp));
        }
        return result;
    }
}