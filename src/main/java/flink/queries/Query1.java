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
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import flink.queries.aggregate.AvgQ1;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import utils.CSVEncoder;
import utils.Config;
import utils.Tools;
import utils.tuples.OutputQuery;

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
        String outputPath = "q1-res";

        var keyed = src
                .filter(event -> event.getSensor_id() < 10000)
//                .setParallelism(2)
                .keyBy(Event::getSensor_id);

        var hourResult = keyed
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
//                .allowedLateness(Time.minutes(2))                                           // funziona
                .aggregate(new AvgQ1(Config.HOUR))
                .setParallelism(4);

        var weekResult = keyed
                .window(TumblingEventTimeWindows.of(Time.days(7)))
//                .allowedLateness(Time.minutes(2))                                           // funziona
                .aggregate(new AvgQ1(Config.WEEK))
                .setParallelism(4);

        //TODO funziona solo con finestra di 30, occorre ragionare sul perché. Con 31 splitta in due finestre, non trovo il senso.
        var monthResult = keyed
                .window(TumblingEventTimeWindows.of(Time.days(30)))
//                .allowedLateness(Time.minutes(2))                                           // funziona
                .aggregate(new AvgQ1(Config.MONTH))
                .setParallelism(4);

        var hourSink = Tools.buildSink("q1-res/hourly");
        var weekSink = Tools.buildSink("q1-res/weekly");
        var monthSink = Tools.buildSink("q1-res/monthly");

        hourResult.addSink(hourSink);               // Il sink deve avere parallelismo 1
        weekResult.addSink(weekSink);               // Il sink deve avere parallelismo 1
        monthResult.addSink(monthSink);             // Il sink deve avere parallelismo 1
        env.execute("Query 1");
    }
}
