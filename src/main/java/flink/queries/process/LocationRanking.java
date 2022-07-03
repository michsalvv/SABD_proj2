package flink.queries.process;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.Tools;
import utils.tuples.OutputQuery;
import utils.tuples.ValQ2;

import java.sql.Timestamp;

public class LocationRanking extends ProcessAllWindowFunction<ValQ2, OutputQuery, TimeWindow> {
    String window;
    Meter meter;

    public LocationRanking(String window) {
        this.window = window;
    }

    @Override
    public void process(ProcessAllWindowFunction<ValQ2, OutputQuery, TimeWindow>.Context context, Iterable<ValQ2> iterable, Collector<OutputQuery> collector){
//        Timestamp end = new Timestamp (context.window().getEnd());
//        Timestamp start = new Timestamp (context.window().getStart());
//        System.out.printf("WINDOW: (%s,%s)\n", new Timestamp(start),new Timestamp(end));

        var ranks = Tools.getLocationsRanking(iterable,window);
        this.meter.markEvent();
        collector.collect(ranks);
    }

    @Override
    public void open(Configuration config) {
        com.codahale.metrics.Meter dropwizardMeter = new com.codahale.metrics.Meter();

        this.meter = getRuntimeContext()
                .getMetricGroup()
                .meter("Throughput", new DropwizardMeterWrapper(dropwizardMeter));
    }
}
