/**
 * Find the real-time top-5 ranking of locations (location) having the highest average temperature
 * and the top-5 ranking of locations (location) having the lowest average temperature
 * --------------------------------------------------------------------------------------
 * Q2 output:
 * ts, location1, avg_temp1, ... location5, avg_temp5, location6, avg
 */

package flink.queries;

import flink.deserialize.Event;
import flink.queries.process.LocationRanking;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import flink.queries.aggregate.AvgQ2;

public class Query2 extends Query {
    StreamExecutionEnvironment env;
    DataStreamSource<Event> src;

    public Query2(StreamExecutionEnvironment env, DataStreamSource<Event> src) {
        this.env = env;
        this.src = src;
    }

    @Override
    public void execute() throws Exception {

        var keyed = src.keyBy(event -> event.getLocation());
        var win = keyed.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        var mean = win.aggregate(new AvgQ2())
                .setParallelism(5);
        var win2 = mean.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));
        var result = win2.process(new LocationRanking())
                .setParallelism(1);
        result.print();
        env.execute("Query 2");
    }
}