package kafka.queries;

import kafka.queries.Windows.WeeklyWindow;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import utils.tuples.Event;
import utils.Tools;
import utils.serdes.CustomSerdes;
import utils.tuples.ValQ2;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Properties;

/**
 * Find the real-time top-5 ranking of locations
 * (location) having the highest average temperature
 * and the top-5 ranking of locations (location) having
 * the lowest average temperature
 * Output:  ts, location1, avg_temp1, ... location5,
 *          avg_temp5, location6, avg_temp6, ...
 *          location10, avg_temp10
 */
public class Query2 extends Query {
    KStream<Long, Event> src;
    StreamsBuilder builder;
    Properties props;

    public Query2(KStream<Long, Event> src, StreamsBuilder builder, Properties props) {
        this.src = src;
        this.builder = builder;
        this.props = props;
    }

    @Override
    public void execute() {

        var keyed = src
                .filter((integer, event) -> event != null)
                .selectKey((integer, event) -> event.getLocation());

        var hourlyGrouped = keyed
                .groupByKey(Grouped.with(Serdes.Long(), CustomSerdes.Event()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofHours(1)));

        var hourlyStatistics = hourlyGrouped
                .aggregate(ValQ2::new, (aLong, event, valQ2) -> {
                    Double temp = valQ2.getTemperature()+event.getTemperature();
                    valQ2.setTemperature(temp);
                    valQ2.setOccurrences(valQ2.getOccurrences()+1L);
                    valQ2.setTimestamp(Tools.getHourSlot(event.getTimestamp()));
                    valQ2.setLocation(event.getLocation());
                    valQ2.calculateMean();
                    return valQ2;
                }, Materialized.with(Serdes.Long(), CustomSerdes.ValQ2()));
        
       var hourlyResult = hourlyStatistics
               .groupBy((longWindowed, valQ2) -> new KeyValue<>(Tools.getHourSlot(valQ2.getTimestamp()).toString(), valQ2),
                       Grouped.with(Serdes.String(), CustomSerdes.ValQ2()))
               .aggregate(LocationAggregator::new, (timestamp, valQ2, aggregator) -> {
                    aggregator.setTimestamp(timestamp);
                    aggregator.updateRank(valQ2);
                    return aggregator;
               }, (timestamp, valQ2, valQ2s) -> null,
                       Materialized.with(Serdes.String(), CustomSerdes.LocationAggregator()));

        var dailyGrouped = keyed
                .groupByKey(Grouped.with(Serdes.Long(), CustomSerdes.Event()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofDays(1)));

        var dailyStatistics = dailyGrouped
                .aggregate(ValQ2::new, (aLong, event, valQ2) -> {
                    Double temp = valQ2.getTemperature()+event.getTemperature();
                    valQ2.setTemperature(temp);
                    valQ2.setOccurrences(valQ2.getOccurrences()+1L);
                    valQ2.setTimestamp(Tools.getDaySlot(event.getTimestamp()));
                    valQ2.setLocation(event.getLocation());
                    valQ2.calculateMean();
                    return valQ2;
                }, Materialized.with(Serdes.Long(), CustomSerdes.ValQ2()));

        var dailyResult = dailyStatistics
                .groupBy((longWindowed, valQ2) -> new KeyValue<>(Tools.getDaySlot(valQ2.getTimestamp()).toString(), valQ2),
                        Grouped.with(Serdes.String(), CustomSerdes.ValQ2()))
                .aggregate(LocationAggregator::new, (timestamp, valQ2, aggregator) -> {
                            aggregator.setTimestamp(timestamp);
                            aggregator.updateRank(valQ2);
                            return aggregator;
                        }, (timestamp, valQ2, valQ2s) -> null,
                        Materialized.with(Serdes.String(), CustomSerdes.LocationAggregator()));

        var weeklyGrouped = keyed
                .groupByKey(Grouped.with(Serdes.Long(), CustomSerdes.Event()))
                .windowedBy(new WeeklyWindow());

        var weeklyStatistics = weeklyGrouped
                .aggregate(ValQ2::new, (aLong, event, valQ2) -> {
                    Double temp = valQ2.getTemperature()+event.getTemperature();
                    valQ2.setTemperature(temp);
                    valQ2.setOccurrences(valQ2.getOccurrences()+1L);
                    valQ2.setTimestamp(Tools.getWeekSlot(event.getTimestamp()));
                    valQ2.setLocation(event.getLocation());
                    valQ2.calculateMean();
                    return valQ2;
                }, Materialized.with(Serdes.Long(), CustomSerdes.ValQ2()));

        var weeklyResult = weeklyStatistics
                .groupBy((longWindowed, valQ2) -> new KeyValue<>(Tools.getWeekSlot(valQ2.getTimestamp()).toString(), valQ2),
                        Grouped.with(Serdes.String(), CustomSerdes.ValQ2()))
                .aggregate(LocationAggregator::new, (timestamp, valQ2, aggregator) -> {
                            aggregator.setTimestamp(timestamp);
                            aggregator.updateRank(valQ2);
                            return aggregator;
                        }, (timestamp, valQ2, valQ2s) -> null,
                        Materialized.with(Serdes.String(), CustomSerdes.LocationAggregator()));

        hourlyResult.toStream().print(Printed.toSysOut());
        dailyResult.toStream().print(Printed.toSysOut());
        weeklyResult.toStream().print(Printed.toSysOut());

        hourlyResult.toStream().to("q2-hourly", Produced.with(Serdes.String(), CustomSerdes.Q2Output()));
        dailyResult.toStream().to("q2-daily", Produced.with(Serdes.String(), CustomSerdes.Q2Output()));
        weeklyResult.toStream().to("q2-weekly", Produced.with(Serdes.String(), CustomSerdes.Q2Output()));


        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp(); //clean up of the local StateStore
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
