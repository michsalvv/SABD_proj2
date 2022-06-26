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

// TODO valutare con i soci se conviene ciclo for dove si setta size=Time.(finestra) e string win = Config.FINESTRA
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
import utils.Tools;
import utils.grid.Cell;
import utils.grid.Grid;
import utils.tuples.OutputQuery;
import utils.tuples.ValQ3;

import java.util.concurrent.TimeUnit;

public class Query3 extends Query {
    StreamExecutionEnvironment env;
    DataStreamSource<Event> src;

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

        var keyed = src
                .filter(event -> isSensorInGrid(event.getLatitude(),event.getLongitude()))
                .map(event -> new ValQ3(event.getTimestamp(),
                        event.getTemperature(), 0D, grid.getCellFromEvent(event).getId()))
                .setParallelism(1)
                .keyBy(v -> v.getCell_id());


        /**
         * Calcolo su finestra di un'ora
         */
        // Calcolo della media
        var hourMean = keyed
                .window(TumblingEventTimeWindows.of(Time.minutes(60)))
                .aggregate(new AvgQ3(Config.HOUR));

        // Calcolo della mediana
        var hourMedian = keyed
                .window(TumblingEventTimeWindows.of(Time.minutes(60)))
                .process(new Median());

        // [MEAN|MEDIAN]
        var hourJoined = hourMean
                .join(hourMedian)
                .where(e->e.getCell_id())
                .equalTo(f->f.getCell_id())
                .window(TumblingEventTimeWindows.of(Time.minutes(60)));

        // Campi di interesse per l'output
        var hourStatistics = hourJoined.apply((JoinFunction<ValQ3, ValQ3, ValQ3>) (v1,v2) -> {
            var id = v1.getCell_id();
            var meanTemp = v1.getMean_temp();
            var medianTemp = v2.getMedian_temp();
            var time = v1.getTimestamp();
            var occ = v1.getOccurrences();
            return new ValQ3(time,meanTemp,medianTemp,id,occ);
        });

        // Unione dei risultati della singola finestra sulla stessa riga
        var hourResult = hourStatistics
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(60)))
                .process(new CellStatistics(Config.HOUR));

        /**
         * Calcolo su finestra di una settimana
         */
        // Calcolo della media
        var weekMean = keyed
                .window(TumblingEventTimeWindows.of(Time.days(7)))
                .aggregate(new AvgQ3(Config.WEEK));

        // Calcolo della mediana
        var weekMedian = keyed
                .window(TumblingEventTimeWindows.of(Time.days(7)))
                .process(new Median());

        // [MEAN|MEDIAN]
        var weekJoined = weekMean
                .join(weekMedian)
                .where(e->e.getCell_id())
                .equalTo(f->f.getCell_id())
                .window(TumblingEventTimeWindows.of(Time.days(7)));

        // Campi di interesse per l'output
        var weekStatistics = weekJoined.apply((JoinFunction<ValQ3, ValQ3, ValQ3>) (v1,v2) -> {
            var id = v1.getCell_id();
            var meanTemp = v1.getMean_temp();
            var medianTemp = v2.getMedian_temp();
            var time = v1.getTimestamp();
            var occ = v1.getOccurrences();
            return new ValQ3(time,meanTemp,medianTemp,id,occ);
        });

        // Unione dei risultati della singola finestra sulla stessa riga
        var weekResult = weekStatistics
                .windowAll(TumblingEventTimeWindows.of(Time.days(7)))
                .process(new CellStatistics(Config.WEEK));

        /**
         * Calcolo su finestra di un mese
         */
        // Calcolo della media
        var monthMean = keyed
                .window(TumblingEventTimeWindows.of(Time.days(30)))
                .aggregate(new AvgQ3(Config.MONTH));

        // Calcolo della mediana
        var monthMedian = keyed
                .window(TumblingEventTimeWindows.of(Time.days(30)))
                .process(new Median());

        // [MEAN|MEDIAN]
        var monthJoined = monthMean
                .join(monthMedian)
                .where(e->e.getCell_id())
                .equalTo(f->f.getCell_id())
                .window(TumblingEventTimeWindows.of(Time.days(30)));

        // Campi di interesse per l'output
        var monthStatistics = monthJoined.apply((JoinFunction<ValQ3, ValQ3, ValQ3>) (v1,v2) -> {
            var id = v1.getCell_id();
            var meanTemp = v1.getMean_temp();
            var medianTemp = v2.getMedian_temp();
            var time = v1.getTimestamp();
            var occ = v1.getOccurrences();
            return new ValQ3(time,meanTemp,medianTemp,id,occ);
        });

        // Unione dei risultati della singola finestra sulla stessa riga
        var monthResult = monthStatistics
                .windowAll(TumblingEventTimeWindows.of(Time.days(30)))
                .process(new CellStatistics(Config.MONTH));

        var hourSink = Tools.buildSink("results/q3-res/hourly");
        var weekSink = Tools.buildSink("results/q3-res/weekly");
        var monthSink = Tools.buildSink("results/q3-res/monthly");

        hourResult.addSink(hourSink);               // Il sink deve avere parallelismo 1
        weekResult.addSink(weekSink);               // Il sink deve avere parallelismo 1
        monthResult.addSink(monthSink);             // Il sink deve avere parallelismo 1

        env.execute("Query 3");
    }

    // LAT (Y): 38 - 58
    // LON (X): 2  - 30
    static boolean isSensorInGrid(Double latitude, Double longitude) {
        if (latitude < 38D || latitude > 58D || longitude < 2D || longitude > 30D) {
            return false;
        }
        return true;
    }
}
