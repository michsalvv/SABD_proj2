package flink.queries.aggregate;

import org.apache.flink.api.common.functions.AggregateFunction;
import utils.Config;
import utils.Tools;
import utils.tuples.ValQ3;

public class AvgQ3 implements AggregateFunction<ValQ3, AccumulatorQ3, ValQ3> {
    String windowType;

    public AvgQ3(String windowType) {
        this.windowType = windowType;
    }

    public AccumulatorQ3 createAccumulator() {
        return new AccumulatorQ3();
    }

    @Override
    public AccumulatorQ3 add(ValQ3 values, AccumulatorQ3 acc) {
        acc.sum += values.getMean_temp();
        acc.count++;
        acc.last_timestamp = values.getTimestamp();
        acc.cell_id = values.getCell_id();
        return acc;
    }

    @Override
    public AccumulatorQ3 merge(AccumulatorQ3 a, AccumulatorQ3 b) {
        a.count += b.count;
        a.sum += b.sum;
        return a;
    }

    @Override
    public ValQ3 getResult(AccumulatorQ3 acc) {
        double mean = acc.sum / (double) acc.count;
        ValQ3 result = new ValQ3();
        result.setMean_temp(mean);
        result.setOccurrences(acc.count);
        result.setCell_id(acc.cell_id);
        if (windowType.equals(Config.HOUR)) {
            result.setTimestamp(Tools.getHourSlot(acc.last_timestamp));
        }
        if (windowType.equals(Config.DAY)) {
            result.setTimestamp(Tools.getDaySlot(acc.last_timestamp));
        }
        if (windowType.equals(Config.WEEK)) {
            result.setTimestamp(Tools.getWeekSlot(acc.last_timestamp));
        }
        return result;
    }
}