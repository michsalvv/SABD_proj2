/**
 * Find the real-time top-5 ranking of locations (location) having the highest average temperature
 * and the top-5 ranking of locations (location) having the lowest average temperature
 * --------------------------------------------------------------------------------------
 * Q2 output:
 * ts, location1, avg_temp1, ... location5, avg_temp5, location6, avg
 * --------------------------------------------------------------------------------------
 * Using a tumbling window, calculate this query:
 * – every 1 hour (event time)
 * – every 1 day (event time)
 * – every 1 week (event time)
 */

package flink.queries;

import flink.deserialize.Event;
import flink.queries.process.LocationRanking;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import flink.queries.aggregate.AvgQ2;
import utils.CSVEncoder;
import utils.Config;
import utils.tuples.OutputQuery;

import java.util.concurrent.TimeUnit;

public class Query2 extends Query {
    StreamExecutionEnvironment env;
    DataStreamSource<Event> src;
    private final static String outputPath = "q2-res";

    public Query2(StreamExecutionEnvironment env, DataStreamSource<Event> src) {
        this.env = env;
        this.src = src;
    }

    @Override
    public void execute() throws Exception {

        var keyed = src.keyBy(event -> event.getLocation())
                .window(TumblingEventTimeWindows.of(Time.minutes(60)));

        var mean = keyed.aggregate(new AvgQ2())
                .setParallelism(5)
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(60)));

        var result = mean.process(new LocationRanking())
                .setParallelism(1);

        StreamingFileSink<OutputQuery> sink = StreamingFileSink
                .forRowFormat(new Path(outputPath), new CSVEncoder())
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMinutes(2))
                                .withInactivityInterval(TimeUnit.MINUTES.toMinutes(1))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .withOutputFileConfig(Config.outputFileConfig)
                .build();

        result.addSink(sink);
        env.execute("Query 2");
    }
}