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
import utils.Tools;
import utils.tuples.OutputQuery;

import java.util.concurrent.TimeUnit;

public class Query2 extends Query {
    StreamExecutionEnvironment env;
    DataStreamSource<Event> src;

    public Query2(StreamExecutionEnvironment env, DataStreamSource<Event> src) {
        this.env = env;
        this.src = src;
    }

    @Override
    public void execute() throws Exception {

        var keyed = src
                .keyBy(event -> event.getLocation());

        var hourResult = keyed
                // Calcolo Media
                .window(TumblingEventTimeWindows.of(Time.minutes(60)))
                .aggregate(new AvgQ2(Config.HOUR))
                .setParallelism(5)
                // Calcolo Ranking
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(60)))
                .process(new LocationRanking(Config.HOUR))
                .setParallelism(1);

        var weekResult = keyed
                // Calcolo Media
                .window(TumblingEventTimeWindows.of(Time.days(7)))
                .aggregate(new AvgQ2(Config.WEEK))
                .setParallelism(5)
                // Calcolo Ranking
                .windowAll(TumblingEventTimeWindows.of(Time.days(7)))
                .process(new LocationRanking(Config.WEEK))
                .setParallelism(1);

        var monthResult = keyed
                // Calcolo Media
                .window(TumblingEventTimeWindows.of(Time.days(30)))
                .aggregate(new AvgQ2(Config.MONTH))
                .setParallelism(5)
                // Calcolo Ranking
                .windowAll(TumblingEventTimeWindows.of(Time.days(30)))
                .process(new LocationRanking(Config.MONTH))
                .setParallelism(1);

        var hourSink = Tools.buildSink("results/q2-res/hourly");
        var weekSink = Tools.buildSink("results/q2-res/weekly");
        var monthSink = Tools.buildSink("results/q2-res/monthly");

        hourResult.addSink(hourSink);               // Il sink deve avere parallelismo 1
        weekResult.addSink(weekSink);               // Il sink deve avere parallelismo 1
        monthResult.addSink(monthSink);             // Il sink deve avere parallelismo 1

        env.execute("Query 2");
    }
}