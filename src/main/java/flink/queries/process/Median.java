package flink.queries.process;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.tuples.ValQ3;
import utils.Tools;

import java.sql.Timestamp;

public class Median extends ProcessWindowFunction<ValQ3, ValQ3, Integer, TimeWindow> {
    @Override
    public void process(Integer cell_id, ProcessWindowFunction<ValQ3, ValQ3, Integer, TimeWindow>.Context context, Iterable<ValQ3> iterable, Collector<ValQ3> collector) throws Exception {
        Timestamp end = new Timestamp(context.window().getEnd());
        Timestamp start = new Timestamp(context.window().getStart());
//        System.out.printf("WINDOW: (%s,%s)\n", start,end);

        var sorted = Tools.sortByTemperature(iterable);
        double median;
        int size = sorted.size();
        if (size % 2 == 0)
            median = (sorted.get(size/2).getMean_temp() + sorted.get(size/2-1).getMean_temp())/2;
        else
            median = sorted.get(size/2).getMean_temp();


        collector.collect(new ValQ3(start,null,median,cell_id));
    }
}
