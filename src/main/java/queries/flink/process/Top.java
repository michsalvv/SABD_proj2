package queries.flink.process;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.ValQ2;

import java.util.List;

public class Top extends ProcessWindowFunction<ValQ2, List<ValQ2>, Long, TimeWindow> {

    @Override
    public void process(Long aLong, Context context, Iterable<ValQ2> iterable, Collector<List<ValQ2>> collector) {
        var res = utils.Tools.getTopFiveLocations(iterable);
        System.out.println("TOP5: " + res);
        collector.collect(res);
    }
}
