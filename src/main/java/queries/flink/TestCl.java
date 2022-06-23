package queries.flink;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.ValQ2;

import java.sql.Timestamp;
import java.util.ArrayList;

public class TestCl extends ProcessAllWindowFunction<ValQ2, ArrayList<ValQ2>, TimeWindow> {
    @Override
    public void process(ProcessAllWindowFunction<ValQ2, ArrayList<ValQ2>, TimeWindow>.Context context, Iterable<ValQ2> iterable, Collector<ArrayList<ValQ2>> collector) throws Exception {
        Long end = context.window().getEnd();
        Long start = context.window().getStart();
        System.out.printf("WINDOW: (%s,%s)\n", new Timestamp(start),new Timestamp(end));


        var l = new ArrayList<ValQ2>();
        iterable.forEach(r->l.add(r));
        collector.collect(l);
    }
}
