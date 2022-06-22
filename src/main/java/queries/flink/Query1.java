package queries.flink;

import flink.Event;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
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
    DataStreamSource<Event> src;

    public Query1(StreamExecutionEnvironment env, DataStreamSource<Event> src) {
        this.env = env;
        this.src = src;
    }

    @Override
    public void execute() throws Exception {
        String outputPath = "query1.csv";

        var dataStream = src
                .filter(event -> event.getSensor_id() < 10000)
                .keyBy(Event::getSensor_id)
                .window(TumblingEventTimeWindows.of(Time.minutes(60), Time.seconds(30)))
                .allowedLateness(Time.minutes(2))                                           // funziona
                .aggregate(new Average())
                .setParallelism(2);

        dataStream.print();
        env.execute("Query 1");

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
