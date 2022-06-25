package kafka.queries;

import flink.deserialize.Event;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.log4j.varia.NullAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Properties;

public class Query1 {

    static final StreamsBuilder builder = new StreamsBuilder();
    private static Logger LOGGER = LoggerFactory.getLogger(Query1.class);

    public static void main(String[] args) {
        final Properties props = new Properties();
        org.apache.log4j.BasicConfigurator.configure(new NullAppender());

        // Give the Streams application a unique name.
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "11adda");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "12312dad" + new Timestamp(System.currentTimeMillis()));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-broker:9092");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");

//        // Records should be flushed every 10 seconds.
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, String.valueOf(0));
//        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        KStream<Long, Event> sourceStream = builder.stream("flink-events", Consumed.with(Serdes.Long(), EventSerde.Event()));
        KTable<Windowed<Long>, Long> output = sourceStream.
                filter((s, event) -> event != null && event.getSensor_id() < 100000)        // Gli danno fastidio gli eventi nulli ovvero quelli che hanno temp negativa, quindi serve sto controllo maledetto
                        .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(30)))
                        .count();


        output.toStream().foreach((aLong, event) -> System.out.println(aLong + " --- " + event));
//        output.foreach((aLong, event) -> System.out.println(aLong + " --- " +event));
//                .selectKey((KeyValueMapper<Long, Event, Long>) (key, event) -> event.getSensor_id());
//
//        output.foreach((aLong, event) -> System.out.println(aLong + " --- " +event));
//

////
//            grouped.count().toStream().foreach((aLong, aLong2) -> System.out.println(aLong + " --------- " + aLong2));
//            KGroupedStream<Long, Event> grouped = output
//                   .groupByKey();

//            var reduced = grouped.reduce(new Reducer<Event>() {
//                @Override
//                public Event apply(Event event, Event v1) {
//                    System.out.println("REDUCE: "+event.toString());
//                    Event e = new Event();
//                    e.setSensor_id(0L);
//                    e.setTemperature(12D);
//                    return e;
//                }
//            });
//            reduced.toStream().to("output", Produced.with(Serdes.Long(), EventSerde.Event()));
/*
        var reduced = grouped.reduce(new Reducer<Event>() {
            @Override
            public Event apply(Event event, Event v1) {
                System.out.println("REDUCE: "+event.toString());
                Event e = new Event();
                e.setSensor_id(0L);
                e.setTemperature(12D);
                return e;
            }
        });
//               .count(Materialized.with(Serdes.Long(), EventSerde.Event()));

//       output.mapValues((aLong, aLong2) -> {
//           System.out.println(aLong + " "+ aLong2);
//           return null;
//       });
//               .groupByKey(Grouped.with(Serdes.Long(), EventSerde.Event()))
//               .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10)))
//               .count(Named.as("dio"))

//               .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10)))
/*

               .aggregate(ValQ1::new, new AggregatorQ1());
*/

////               .groupBy((aLong, event) -> aLong, Grouped.with(Serdes.Long(), EventSerde.Event()))
        ;
//        output.toStream().foreach((longWindowed, valQ1) -> System.out.println(valQ1.toString()));
//        output.toStream().foreach((longWindowed, aLong) -> System.out.println(longWindowed));
//            output.print(Printed.toSysOut());
//        output.toStream().foreach((o, o2) -> System.out.println(o.toString()));
//        output.toStream().to("output", Produced.with(Serdes.Long(), EventSerde.Event()));

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp();
        streams.start();
//        streams.close();
    }
}
