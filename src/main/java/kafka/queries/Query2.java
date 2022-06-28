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
import utils.serdes.CustomSerdes;
import utils.tuples.ValQ1;
import utils.tuples.ValQ2;

import java.time.Duration;
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
                .windowedBy(new MonthlyWindow());

        var result = grouped
                .aggregate(ValQ2::new, (aLong, event, valQ2) -> {
                    System.out.println(event);
                    System.out.println(valQ2);
                    System.out.println("--------------");

                    Double temp = valQ2.getMeanTemperature()+event.getTemperature();
                    valQ2.addOccurrences();
                    valQ2.setMeanTemperature(temp);
                    valQ2.setTimestamp(event.getTimestamp());
                    valQ2.setLocation(event.getLocation());
                    return valQ2;
                }, Materialized.with(Serdes.Long(), CustomSerdes.ValQ2()));


        result.toStream().print(Printed.toSysOut());




        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp(); //clean up of the local StateStore
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
