package kafka.queries;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import utils.Event;
import utils.serdes.CustomSerdes;
import utils.tuples.ValQ1;
import java.time.Duration;
import java.util.Properties;

public class Query1 extends Query {
    KStream<Integer, Event> src;
    StreamsBuilder builder;
    Properties props;

    public Query1(KStream<Integer, Event> src, StreamsBuilder builder, Properties props) {
        this.src = src;
        this.builder = builder;
        this.props = props;
    }

    @Override
    public void execute() {
        String outputPath = "q1-res";

        var grouped = src
                .filter((key, event) -> event!=null && event.getSensor_id() < 100000)
                .map((KeyValueMapper<Integer, Event, KeyValue<Integer, ValQ1>>) (integer, event) -> {
                    ValQ1 v = new ValQ1();
                    v.setTimestamp(event.getTimestamp());
                    v.setTemperature(event.getTemperature());
                    v.setSensor_id(event.getSensor_id());
                    v.setOccurrences(1L);
                    return new KeyValue<>(integer, v);
                });

        var keyed = grouped
                .selectKey((key, values) -> values.getSensor_id())
                .groupByKey(Grouped.with(Serdes.Long(), CustomSerdes.ValQ1()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(10)));

        var reduced = keyed.reduce((v1, v2) -> {
            Long occ = v1.getOccurrences() + v2.getOccurrences();
            Double temp = v1.getTemperature() + v2.getTemperature();
            return new ValQ1(v2.getTimestamp(), v2.getSensor_id(), temp, occ);
        });

        var results = reduced
                .mapValues(valQ1 -> {
                    Double meanTemperature = valQ1.getTemperature()/(double)valQ1.getOccurrences();
                    valQ1.setTemperature(meanTemperature);
                    return valQ1;
                });

        results.toStream().print(Printed.toSysOut());
        results.toStream().to("output", Produced.with(
                  WindowedSerdes.timeWindowedSerdeFrom(Long.class, Long.MAX_VALUE), CustomSerdes.ValQ1()));

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp(); //clean up of the local StateStore
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
