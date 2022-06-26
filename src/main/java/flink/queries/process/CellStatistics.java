package flink.queries.process;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.Tools;
import utils.tuples.OutputQuery;
import utils.tuples.ValQ3;

import java.sql.Timestamp;

public class CellStatistics extends ProcessAllWindowFunction<ValQ3, OutputQuery, TimeWindow> {
    String window;

    public CellStatistics(String window) {
        this.window = window;
    }

    @Override
    public void process(ProcessAllWindowFunction<ValQ3, OutputQuery, TimeWindow>.Context context, Iterable<ValQ3> iterable, Collector<OutputQuery> collector) throws Exception {
        Timestamp end = new Timestamp(context.window().getEnd());
        Timestamp start = new Timestamp(context.window().getStart());
//        System.out.printf("CELL STATISTICS WINDOW: (%s,%s)\n", start, end);
        OutputQuery stats = Tools.cellStatisticsOnRow(iterable, start, window);

        collector.collect(stats);
}
}
