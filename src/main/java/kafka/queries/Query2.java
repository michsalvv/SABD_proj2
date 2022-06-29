package kafka.queries;

import kafka.queries.Windows.MonthlyWindow;
import kafka.queries.Windows.WeeklyWindow;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import utils.Event;
import utils.Tools;
import utils.serdes.ArrayListSerde;
import utils.serdes.CustomSerdes;
import utils.tuples.ValQ2;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
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
    KStream<Integer, Event> src;
    StreamsBuilder builder;
    Properties props;

    public Query2(KStream<Integer, Event> src, StreamsBuilder builder, Properties props) {
        this.src = src;
        this.builder = builder;
        this.props = props;
    }

    @Override
    public void execute() {

        var keyed = src
                .filter((integer, event) -> event != null)
                .selectKey((integer, event) -> event.getLocation());


        var grouped = keyed
                .groupByKey(Grouped.with(Serdes.Long(), CustomSerdes.Event()))
                .windowedBy(new WeeklyWindow());

        var statistics = grouped
                .aggregate(ValQ2::new, (aLong, event, valQ2) -> {
                    Double temp = valQ2.getTemperature()+event.getTemperature();
                    valQ2.setTemperature(temp);
                    valQ2.setOccurrences(valQ2.getOccurrences()+1L);
                    valQ2.setTimestamp(event.getTimestamp());
                    valQ2.setLocation(event.getLocation());
                    valQ2.calculateMean();
                    return valQ2;
                }, Materialized.with(Serdes.Long(), CustomSerdes.ValQ2()));

        statistics.toStream().print(Printed.toSysOut());

       var result = statistics
               .groupBy((longWindowed, valQ2) -> new KeyValue<>(Tools.getWeekSlot(valQ2.getTimestamp()).toString(), valQ2),
                       Grouped.with(Serdes.String(), CustomSerdes.ValQ2()))
               .aggregate(ArrayList::new, (Aggregator<String, ValQ2, ArrayList<ValQ2>>) (timestamp, valQ2, valQ2s) -> {


                   valQ2s.add(valQ2);
                   return valQ2s;
               }, (Aggregator<String, ValQ2, ArrayList<ValQ2>>) (timestamp, valQ2, valQ2s) -> null,
                       Materialized.with(Serdes.String(), new ArrayListSerde<>(CustomSerdes.ValQ2()))
               );

        result.toStream().print(Printed.toSysOut());

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp(); //clean up of the local StateStore
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
