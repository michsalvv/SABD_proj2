package flink.queries.process;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.tuples.ValQ3;

public class MedianQ3 extends ProcessWindowFunction<ValQ3, ValQ3, Integer, TimeWindow> {
    @Override
    public void process(Integer integer, ProcessWindowFunction<ValQ3, ValQ3, Integer, TimeWindow>.Context context, Iterable<ValQ3> iterable, Collector<ValQ3> collector) throws Exception {
        // TODO Sort e Mediana
        // TODO Unione con la Media
    }
}
