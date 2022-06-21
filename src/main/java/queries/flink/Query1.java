package queries.flink;

import queries.flink.aggregate.Average;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import queries.Query;
import utils.ValQ1;

import java.time.Duration;

public class Query1 extends Query {
    StreamExecutionEnvironment env;
    DataStreamSource<String> src;

    public Query1(StreamExecutionEnvironment env, DataStreamSource<String> src) {
        this.env = env;
        this.src = src;
    }

    //TODO Fare un serializzatore vero
    @Override
    public void execute() throws Exception {
        var dataStream = src
                .map(values -> Tuple2.of(ValQ1.create(values), 1))
                .returns(Types.TUPLE(Types.GENERIC(ValQ1.class), Types.INT))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple2<ValQ1, Integer>>forBoundedOutOfOrderness(Duration.ofMinutes(1))                          // Assumiamo il dataset ordinato
                        .withTimestampAssigner((tuple, timestamp) -> tuple.f0.getTimestamp().getTime())
                        .withIdleness(Duration.ofMinutes(1))
                        )
                .keyBy(values -> values.f0.getSensor_id())
                .window(TumblingEventTimeWindows.of(Time.minutes(60)))
                .aggregate(new Average());
        dataStream.print();
        env.execute("Kafka Connector Demo");
    }
}
