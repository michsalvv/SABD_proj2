package queries.flink.process;

import flink.Event;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.ValQ2;

import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;

public class Top extends ProcessWindowFunction<Event, Event, Long, TimeWindow> {

//    @Override
//    public void process(Long aLong, Context context, Iterable<ValQ2> iterable, Collector<List<ValQ2>> collector) {
//        var res = utils.Tools.getTopFiveLocations(iterable);
//        System.out.println("TOP5: " + res);
//        collector.collect(res);
//    }

    @Override
    public void process(Long aLong, ProcessWindowFunction<Event, Event, Long, TimeWindow>.Context context, Iterable<Event> iterable, Collector<Event> collector) throws Exception {
        Iterator<Event> iterator = iterable.iterator();
        Long end = context.window().getEnd();
        Long start = context.window().getStart();
        System.out.printf("WINDOWS: (%s,%s)", new Timestamp(start),new Timestamp(end));

        Event max = null;
        Double maxVal = 0D;

        while (iterator.hasNext()) {
            Event element = iterator.next();
            System.out.println("ITERAZIONE: " + element);
            Event actual = element;
            Double actualVal = element.getTemperature();
            if (actualVal >= maxVal) {
                maxVal = actualVal;
                max = actual;
            }
        }
        collector.collect(max);
    }
}
