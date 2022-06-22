package queries.flink;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.ValQ2;

import java.sql.Timestamp;
import java.util.List;

public class TopAll extends ProcessAllWindowFunction<ValQ2, List<ValQ2>, TimeWindow> {
    @Override
    public void process(ProcessAllWindowFunction<ValQ2, List<ValQ2>, TimeWindow>.Context context, Iterable<ValQ2> iterable, Collector<List<ValQ2>> collector) throws Exception {
        Long end = context.window().getEnd();
        Long start = context.window().getStart();
        System.out.printf("WINDOW: (%s,%s)\n", new Timestamp(start),new Timestamp(end));
        var top = utils.Tools.getTopFiveLocations(iterable);
        collector.collect(top);
    }
}
