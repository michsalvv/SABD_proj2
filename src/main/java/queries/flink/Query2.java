/**
 * Find the real-time top-5 ranking of locations (location) having the highest average temperature
 * and the top-5 ranking of locations (location) having the lowest average temperature
 * --------------------------------------------------------------------------------------
 * Q2 output:
 * ts, location1, avg_temp1, ... location5, avg_temp5, location6, avg
 */

package queries.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import queries.Query;
import queries.flink.aggregate.Average;
import queries.flink.aggregate.Average2;
import utils.ValQ2;

import java.time.Duration;

public class Query2 extends Query {
    StreamExecutionEnvironment env;
    DataStreamSource<String> src;

    public Query2(StreamExecutionEnvironment env, DataStreamSource<String> src) {
        this.env = env;
        this.src = src;
    }

    @Override
    public void execute() throws Exception {
        var dataStream = src
                .map(values -> Tuple2.of(ValQ2.create(values), 1))
                .returns(Types.TUPLE(Types.GENERIC(ValQ2.class), Types.INT))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple2<ValQ2, Integer>>forBoundedOutOfOrderness(Duration.ofMinutes(1))                          // Assumiamo il dataset ordinato
                        .withTimestampAssigner((tuple, timestamp) -> tuple.f0.getTimestamp().getTime())
                        .withIdleness(Duration.ofMinutes(1))
                        )
                .keyBy(values -> values.f0.getLocation())
                .window(TumblingEventTimeWindows.of(Time.minutes(60)))
                .aggregate(new Average2());
        dataStream.print();
        env.execute("Query 2");
    }
}
