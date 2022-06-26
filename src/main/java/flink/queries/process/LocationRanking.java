package flink.queries.process;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.Tools;
import utils.tuples.OutputQuery;
import utils.tuples.ValQ2;

import java.sql.Timestamp;

public class LocationRanking extends ProcessAllWindowFunction<ValQ2, OutputQuery, TimeWindow> {
    String window;

    public LocationRanking(String window) {
        this.window = window;
    }

    @Override
    public void process(ProcessAllWindowFunction<ValQ2, OutputQuery, TimeWindow>.Context context, Iterable<ValQ2> iterable, Collector<OutputQuery> collector){
        Timestamp end = new Timestamp (context.window().getEnd());
        Timestamp start = new Timestamp (context.window().getStart());
//        System.out.printf("WINDOW: (%s,%s)\n", new Timestamp(start),new Timestamp(end));

        var ranks = Tools.getLocationsRanking(iterable,window);

        collector.collect(ranks);
    }
}
