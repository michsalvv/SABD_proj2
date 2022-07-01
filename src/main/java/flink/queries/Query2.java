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

import utils.tuples.Event;
import flink.queries.process.LocationRanking;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import flink.queries.aggregate.AvgQ2;
import utils.Config;
import utils.Tools;

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
                .keyBy(Event::getLocation);

        var hourResult = keyed
                // Calcolo Media
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new AvgQ2(Config.HOUR))
                .name("Hourly Window Mean AggregateFunction")
                // Calcolo Ranking
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                .process(new LocationRanking(Config.HOUR))
                .name("Hourly Window Ranking ProcessFunction")
                .setParallelism(1)
                .disableChaining();

        var dayResult = keyed
                // Calcolo Media
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new AvgQ2(Config.DAY))
                .name("Daily Window Mean AggregateFunction")
                // Calcolo Ranking
                .windowAll(TumblingEventTimeWindows.of(Time.days(1)))
                .process(new LocationRanking(Config.DAY))
                .name("Daily Window Ranking ProcessFunction")
                .setParallelism(1)
                .disableChaining();

        var weekResult = keyed
                // Calcolo Media
                .window(TumblingEventTimeWindows.of(Time.days(7),Time.days(3)))
                .aggregate(new AvgQ2(Config.WEEK))
                .name("Weekly Window Mean AggregateFunction")
                // Calcolo Ranking
                .windowAll(TumblingEventTimeWindows.of(Time.days(7),Time.days(3)))
                .process(new LocationRanking(Config.WEEK))
                .name("Weekly Window Ranking ProcessFunction")
                .setParallelism(1)
                .disableChaining();


        var hourSink = Tools.buildSink("results/q2-res/hourly");
        var daySink = Tools.buildSink("results/q2-res/daily");
        var weekSink = Tools.buildSink("results/q2-res/weekly");

        hourResult.addSink(hourSink).name("Hourly CSV").setParallelism(1);               // Il sink deve avere parallelismo 1
        dayResult.addSink(daySink).name("Daily CSV").setParallelism(1);                 // Il sink deve avere parallelismo 1
        weekResult.addSink(weekSink).name("Weekly CSV").setParallelism(1);               // Il sink deve avere parallelismo 1

        env.execute("Query 2");
    }
}