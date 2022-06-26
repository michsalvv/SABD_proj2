/**
 * Consider the latitude and longitude coordinates
 * the latitude and longitude coordinates (38 , 2 ) and
 * within the geographic area which is identified from
 * • Divide this area using a 4x4 grid and identify each
 * (58 , 30 ).
 * grid cell from the top-left to bottom-right corners using
 * the name "cell_X", where X is the cell id from 0 to 15.
 * For each cell, find the average and the median
 * temperature, taking into account the values emitted
 * from the sensors which are located inside that cell
 * -------------------------------------------------------------
 * Q3 output:
 * ts, cell_0, avg_temp0, med_temp0, ...
 * cell_15, avg_temp15, med_temp15
 * -------------------------------------------------------------
 * Using a tumbling window, calculate this query:
 * – every 1 hour (event time)
 * – every 1 day (event time)
 * – every 1 week (event time)
 */

// TODO finire windowing per tutte le taglie.
package flink.queries;

import flink.deserialize.Event;
import flink.queries.aggregate.AvgQ3;
import flink.queries.process.CellStatistics;
import flink.queries.process.Median;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import utils.CSVEncoder;
import utils.Config;
import utils.grid.Cell;
import utils.grid.Grid;
import utils.tuples.OutputQuery;
import utils.tuples.ValQ3;

import java.util.concurrent.TimeUnit;

public class Query3 extends Query {
    StreamExecutionEnvironment env;
    DataStreamSource<Event> src;
    private final static String outputPath = "q3-res";


    public Query3(StreamExecutionEnvironment env, DataStreamSource<Event> src) {
        this.env = env;
        this.src = src;
    }

    @Override
    public void execute() throws Exception {
        Double lat1 = 38.0;
        Double lon1 = 2.0;
        Double lat2 = 58.0;
        Double lon2 = 30.0;

        // construct 4x4 grid defined by the two points: (lat, lon) = (38°,2°) - (58°,30°)
        // this means that top-left = (58°,2°) and bottom_right = (38°,30°)
        Grid grid = new Grid(lat1, lon1, lat2, lon2, Config.SPLIT_FACTOR);

        var dataStream = src
                .filter(event -> validateCoordinates(event.getLatitude(),event.getLongitude()));

        var mapped = dataStream.map(event -> {
                    Cell c = grid.getCellFromEvent(event);
                    return new ValQ3(event.getTimestamp(),
                            event.getTemperature(), 0D, c.getId());
                }).setParallelism(1);

        var keyed = mapped
                .keyBy(v -> v.getCell_id());

        var median = keyed
                .window(TumblingEventTimeWindows.of(Time.minutes(60)))
                .process(new Median());

        var mean = keyed
                .window(TumblingEventTimeWindows.of(Time.minutes(60)))
                .aggregate(new AvgQ3());

        // [MEAN|MEDIAN]
        var joined = mean
                .join(median)
                .where(e->e.getCell_id())
                .equalTo(f->f.getCell_id())
                .window(TumblingEventTimeWindows.of(Time.minutes(60)));


        var statistics = joined.apply((JoinFunction<ValQ3, ValQ3, ValQ3>) (v1,v2) -> {
            var id = v1.getCell_id();
            var meanTemp = v1.getMean_temp();
            var medianTemp = v2.getMedian_temp();
            var time = v1.getTimestamp();
            var occ = v1.getOccurrences();
            return new ValQ3(time,meanTemp,medianTemp,id,occ);
        });

        var resultRow = statistics
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(60)))
                .process(new CellStatistics(Config.HOUR));

//        resultRow.print();

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

        resultRow.addSink(sink);
        env.execute("Query 3");
    }

    // LAT Y: 38 - 58
    // LON X: 2  - 30
    static boolean validateCoordinates(Double latitude, Double longitude) {
        if (latitude < 38D || latitude > 58D || longitude < 2D || longitude > 30D) {
            return false;
        }
        return true;
    }
}
