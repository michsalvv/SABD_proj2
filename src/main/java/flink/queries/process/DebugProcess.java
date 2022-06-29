package flink.queries.process;

import flink.deserialize.Event;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.tuples.OutQ1;
import utils.tuples.ValQ2;
import utils.tuples.ValQ3;

import java.sql.Timestamp;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

public class DebugProcess extends ProcessWindowFunction<Event, Tuple2<Long,Integer>, Long, TimeWindow> {
    @Override
    public void process(Long sensor_id, ProcessWindowFunction<Event, Tuple2<Long,Integer>, Long, TimeWindow>.Context context, Iterable<Event> iterable, Collector<Tuple2<Long,Integer>> collector) throws Exception {
        Timestamp end = new Timestamp(context.window().getEnd());
        Timestamp start = new Timestamp(context.window().getStart());
        System.out.printf("WINDOW: (%s,%s)\n", start,end);
        Iterator<Event> iterator = iterable.iterator();
        Integer count = 0;
        while (iterator.hasNext()) {
            count++;
        }
        collector.collect(new Tuple2<>(sensor_id,count));
    }
}
