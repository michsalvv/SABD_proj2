/**
 * Find the real-time top-5 ranking of locations (location) having the highest average temperature
 * and the top-5 ranking of locations (location) having the lowest average temperature
 * --------------------------------------------------------------------------------------
 * Q2 output:
 * ts, location1, avg_temp1, ... location5, avg_temp5, location6, avg
 */

package queries.flink;

import flink.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import queries.Query;
import queries.flink.aggregate.Average2;
import queries.flink.process.Top;
import utils.Tools;
import utils.ValQ2;

import java.sql.Timestamp;
import java.time.Duration;

public class Query2 extends Query {
    StreamExecutionEnvironment env;
    DataStreamSource<Event> src;

    public Query2(StreamExecutionEnvironment env, DataStreamSource<Event> src) {
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


