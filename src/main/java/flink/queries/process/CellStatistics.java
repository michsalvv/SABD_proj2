package flink.queries.process;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.Tools;
import utils.tuples.OutputQ2;
import utils.tuples.OutputQuery;
import utils.tuples.ValQ2;
import utils.tuples.ValQ3;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CellStatistics extends ProcessAllWindowFunction<ValQ3, OutputQuery, TimeWindow> {
    @Override
    public void process(ProcessAllWindowFunction<ValQ3, OutputQuery, TimeWindow>.Context context, Iterable<ValQ3> iterable, Collector<OutputQuery> collector) throws Exception {
        Long end = context.window().getEnd();
        Long start = context.window().getStart();
        System.out.printf("CELL STATISTICS WINDOW: (%s,%s)\n", new Timestamp(start), new Timestamp(end));
        OutputQuery stats = Tools.cellStatisticsOnRow(iterable);

        collector.collect(stats);
}
}
