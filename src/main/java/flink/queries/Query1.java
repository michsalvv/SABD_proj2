/**
 * For those sensors having sensor_id < 10000, find
 * the number of measurements and the temperature
 * average value
 * -----------------------------------------------------
 * Q1 output:
 * ts, sensor_id, count, avg_temperature
 * -----------------------------------------------------
 * Using a tumbling window, calculate this query:
 * – every 1 hour (event time)
 * – every 1 week (event time)
 * – from the beginning of the dataset
 */
// TODO vedere i parallelismi su tutte le query
package flink.queries;

import flink.deserialize.Event;
import flink.queries.aggregate.AvgQ1;
import flink.queries.process.DebugProcess;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import utils.Config;
import utils.Tools;

public class Query1 extends Query {
    StreamExecutionEnvironment env;
    DataStreamSource<Event> src;

    public Query1(StreamExecutionEnvironment env, DataStreamSource<Event> src) {
        this.env = env;
        this.src = src;
    }

    @Override
    public void execute() throws Exception {

        var keyed = src
                .filter(event -> event.getSensor_id() < 10000)
                .keyBy(Event::getSensor_id);

        var hourResult = keyed
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new AvgQ1(Config.HOUR))
                .name("Hourly Window Mean AggregateFunction");

        var weekResult = keyed
                .window(TumblingEventTimeWindows.of(Time.days(7),Time.days(3)))
                .aggregate(new AvgQ1(Config.WEEK))
                .name("Weekly Window Mean AggregateFunction");

        var monthResult = keyed
                .window(TumblingEventTimeWindows.of(Time.days(31),Time.days(17)))
                .aggregate(new AvgQ1(Config.MONTH))
                .name("Monthly Window Mean AggregateFunction");

        var hourSink = Tools.buildSink("results/q1-res/hourly");
        var weekSink = Tools.buildSink("results/q1-res/weekly");
        var monthSink = Tools.buildSink("results/q1-res/monthly");
//
        hourResult.addSink(hourSink).name("Hourly CSV").setParallelism(1);               // Il sink deve avere parallelismo 1
        weekResult.addSink(weekSink).name("Weekly CSV").setParallelism(1);               // Il sink deve avere parallelismo 1
        monthResult.addSink(monthSink).name("Monthly CSV").setParallelism(1);             // Il sink deve avere parallelismo 1
        env.execute("Query 1");
    }
}
