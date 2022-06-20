package flink;

import org.apache.flink.api.common.functions.AggregateFunction;
import utils.ValQ1;

public class AggregatorQuery1 implements AggregateFunction<ValQ1, AccumulatorQuery1, Double> {

    @Override
    public AccumulatorQuery1 createAccumulator() {
        return new AccumulatorQuery1();
    }

    @Override
    public AccumulatorQuery1 add(ValQ1 valQ1, AccumulatorQuery1 acc) {
        acc.sum += valQ1.getTemperature();
        acc.count++;
        return acc;
    }

    @Override
    public Double getResult(AccumulatorQuery1 acc) {
        return acc.sum / (double) acc.count;
    }

    @Override
    public AccumulatorQuery1 merge(AccumulatorQuery1 accumulatorQuery1, AccumulatorQuery1 acc1) {
        accumulatorQuery1.count += acc1.count;
        accumulatorQuery1.sum += acc1.sum;
        return accumulatorQuery1;
    }
}
