/**
 * Consider the latitude and longitude coordinates
 * within the geographic area which is identified from
 * the latitude and longitude coordinates (38 , 2 ) and
 * (58 , 30 ).
 * • Divide this area using a 4x4 grid and identify each
 * grid cell from the top-left to bottom-right corners using
 * the name "cell_X", where X is the cell id from 0 to 15.
 * For each cell, find the average and the median
 * temperature, taking into account the values emitted
 * from the sensors which are located inside that cell
 * -------------------------------------------------------------
 * Q3 output:
 * ts, cell_0, avg_temp0, med_temp0, ...
 * cell_15, avg_temp15, med_temp15
 */

package queries.flink;

import flink.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import queries.Query;
import queries.flink.aggregate.Average2;

public class Query3 extends Query {
    StreamExecutionEnvironment env;
    DataStreamSource<Event> src;

    public Query3(StreamExecutionEnvironment env, DataStreamSource<Event> src) {
        this.env = env;
        this.src = src;
    }

    @Override
    public void execute() throws Exception {
//        var dataStream = src
//                .map(values -> Tuple2.of(ValQ2.create(values), 1))
//                .returns(Types.TUPLE(Types.GENERIC(ValQ2.class), Types.INT))
//                .assignTimestampsAndWatermarks(WatermarkStrategy
//                        .<Tuple2<ValQ2, Integer>>forBoundedOutOfOrderness(Duration.ofMinutes(1))                          // Assumiamo il dataset ordinato
//                        .withTimestampAssigner((tuple, timestamp) -> tuple.f0.getTimestamp().getTime())
//                        .withIdleness(Duration.ofMinutes(1)))
//
//                .keyBy(values -> values.f0.getLocation())
//                .window(TumblingEventTimeWindows.of(Time.minutes(60)))
//                .aggregate(new Average2(), new Top());
//                .aggregate(new Average2());

//        var dataStream = src
        var keyed = src.keyBy(event -> event.getLocation());
//        var mapped = src.map(e->new ValQ2(Tools.getSecondsSlot(e.getTimestamp(),10),e.getLocation(),e.getTemperature(),1L));
////        mapped.windowAll(TumblingEventTimeWindows.of(Time.seconds(10))).process(new TestCl()).print();
//
//        var keyed    = mapped.keyBy(e -> e.getLocation());
////        keyed.windowAll(TumblingEventTimeWindows.of(Time.seconds(10))).process(new TestCl()).print();
////        keyed.print();
//
//
//        var reduced = keyed.reduce(new ReduceFunction<ValQ2>() {
//                    @Override
//                    public ValQ2 reduce(ValQ2 v1, ValQ2 v2) throws Exception {
//                        ValQ2 v = new ValQ2();
//                        Timestamp ts = Tools.getSecondsSlot(v1.getTimestamp(),10);
//                        v.setTemperature(v1.getTemperature()+ v2.getTemperature());
//                        v.setOccurrences(v1.getOccurrences()+ v2.getOccurrences());
//                        v.setLocation(v1.getLocation());
//                        v.setTimestamp(ts);
//                        return v;
//                    }
//                });
//        reduced.print();
//        reduced.windowAll(TumblingEventTimeWindows.of(Time.seconds(10))).process(new TestCl()).print();

//        reduced.print();

//        var mean = reduced.map(new MapFunction<ValQ2, ValQ2>() {
//                    @Override
//                    public ValQ2 map(ValQ2 v) throws Exception {
//                        Double temp = v.getTemperature();
//                        Long occur = v.getOccurrences();
//                        Double mean = temp/occur;
//                        v.setTemperature(mean);
//                        return v;
//                    }
//                });
//
//        var window= mean.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));
//        var processed = window.process(new TopAll());
//        processed.print();
        var win = keyed.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        var mean = win.aggregate(new Average2()).setParallelism(5);
        var win2 = mean.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));
        var result = win2.process(new TopAll());
        result.setParallelism(1);

        result.print();

//                .window(TumblingEventTimeWindows.of(Time.minutes(60)))
//                .process(new Top());
//                .aggregate(new Average2());
//                .aggregate(new Average2());

        env.execute("Query 2");
    }
}


