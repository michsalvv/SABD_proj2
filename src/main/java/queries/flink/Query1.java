package queries.flink;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
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
import java.util.concurrent.TimeUnit;

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
        String outputPath = "query1.csv";

        var dataStream = src
                .map(values -> Tuple2.of(ValQ1.create(values), 1))
                .returns(Types.TUPLE(Types.GENERIC(ValQ1.class), Types.INT))
                .filter(values -> values.f0.getSensor_id() < 10000)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple2<ValQ1, Integer>>forMonotonousTimestamps()                   // Assumiamo il dataset ordinato
                        .withTimestampAssigner((tuple, timestamp) -> tuple.f0.getTimestamp().getTime())
                        .withIdleness(Duration.ofMinutes(1))
                        )
                .keyBy(values -> values.f0.getSensor_id())
                .window(TumblingEventTimeWindows.of(Time.minutes(60)))
                .aggregate(new Average());
        dataStream.print();

        OutputFileConfig config = OutputFileConfig
                .builder()
                .withPartPrefix("part")
                .withPartSuffix(".inprogress")
                .build();

        final StreamingFileSink<ValQ1> sink = StreamingFileSink
                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<ValQ1>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMinutes(1))
                                .withInactivityInterval(TimeUnit.MINUTES.toMinutes(1))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .withOutputFileConfig(config)
                .build();
        dataStream.addSink(sink);
        env.execute("Kafka Connector Demo");
    }
}
