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
package flink.queries;

import flink.deserialize.Event;
import flink.metrics.ThroughputMetricQ1;
import flink.queries.aggregate.AvgQ1;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
        System.out.println("Start Time: " + Tools.getTimestamp());

        var keyed = src
                .filter(event -> event.getSensor_id() < 10000)
                .keyBy(Event::getSensor_id);

        var hourResult = keyed
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new AvgQ1(Config.HOUR))
                .name("Hourly Window Mean AggregateFunction")
                .disableChaining();

        hourResult.map(new ThroughputMetricQ1())
                .uid("hour-throughput")
                .name("HourMetric")
                .setParallelism(1);

        var weekResult = keyed
                .window(TumblingEventTimeWindows.of(Time.days(7),Time.days(3)))
                .aggregate(new AvgQ1(Config.WEEK))
                .name("Weekly Window Mean AggregateFunction")
                .disableChaining();

        weekResult.map(new ThroughputMetricQ1())
                .uid("week-throughput")
                .name("WeekMetric")
                .setParallelism(1);

        var monthResult = keyed
                .window(TumblingEventTimeWindows.of(Time.days(31),Time.days(17)))
                .aggregate(new AvgQ1(Config.MONTH))
                .name("Monthly Window Mean AggregateFunction")
                .disableChaining();


        monthResult.map(new ThroughputMetricQ1())
                .uid("month-throughput")
                .name("MonthMetric")
                .setParallelism(1);

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
