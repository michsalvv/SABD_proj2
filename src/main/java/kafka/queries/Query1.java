package kafka.queries;

import kafka.queries.Windows.MonthlyWindow;
import kafka.queries.Windows.WeeklyWindow;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import utils.tuples.Event;
import utils.Tools;
import utils.serdes.CustomSerdes;
import utils.tuples.ValQ1;

import java.time.Duration;
import java.util.Properties;

public class Query1 extends Query {
    KStream<Long, Event> src;
    StreamsBuilder builder;
    Properties props;

    public Query1(KStream<Long, Event> src, StreamsBuilder builder, Properties props) {
        this.src = src;
        this.builder = builder;
        this.props = props;
    }

    @Override
    public void execute() {

        var keyed = src
                .filter((key, event) -> event!=null && event.getSensor_id() < 10000)
                .map((KeyValueMapper<Long, Event, KeyValue<Long, ValQ1>>) (along, event) -> {
                    ValQ1 v = new ValQ1();
                    v.setTimestamp(event.getTimestamp());
                    v.setTemperature(event.getTemperature());
                    v.setSensor_id(event.getSensor_id());
                    v.setOccurrences(1L);
                    return new KeyValue<>(along, v);
                })
                .selectKey((key, values) -> values.getSensor_id());

        var monthlyGrouped = keyed
                .groupByKey(Grouped.with(Serdes.Long(), CustomSerdes.ValQ1()))
                .windowedBy(new MonthlyWindow())
                .reduce((v1, v2) -> {
                    Long occ = v1.getOccurrences() + v2.getOccurrences();
                    Double temp = v1.getTemperature() + v2.getTemperature();
                    return new ValQ1(Tools.getMonthSlot(v2.getTimestamp()), v2.getSensor_id(), temp, occ);
                })
                .mapValues(valQ1 -> {
                    Double meanTemperature = valQ1.getTemperature()/(double)valQ1.getOccurrences();
                    valQ1.setTemperature(meanTemperature);
                    return valQ1;
                });

        var weeklyGrouped = keyed
                .groupByKey(Grouped.with(Serdes.Long(), CustomSerdes.ValQ1()))
                .windowedBy(new WeeklyWindow())
                .reduce((v1, v2) -> {
                    Long occ = v1.getOccurrences() + v2.getOccurrences();
                    Double temp = v1.getTemperature() + v2.getTemperature();
                    return new ValQ1(Tools.getWeekSlot(v2.getTimestamp()), v2.getSensor_id(), temp, occ);
                })
                .mapValues(valQ1 -> {
                    Double meanTemperature = valQ1.getTemperature()/(double)valQ1.getOccurrences();
                    valQ1.setTemperature(meanTemperature);
                    return valQ1;
                });

        var hourlyGrouped = keyed
                .groupByKey(Grouped.with(Serdes.Long(), CustomSerdes.ValQ1()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofHours(1)))
                .reduce((v1, v2) -> {
                    Long occ = v1.getOccurrences() + v2.getOccurrences();
                    Double temp = v1.getTemperature() + v2.getTemperature();
                    return new ValQ1(Tools.getHourSlot(v2.getTimestamp()), v2.getSensor_id(), temp, occ);
                })
                .mapValues(valQ1 -> {
                    Double meanTemperature = valQ1.getTemperature()/(double)valQ1.getOccurrences();
                    valQ1.setTemperature(meanTemperature);
                    return valQ1;
                });

        hourlyGrouped.toStream().print(Printed.toSysOut());
        weeklyGrouped.toStream().print(Printed.toSysOut());
        monthlyGrouped.toStream().print(Printed.toSysOut());

        monthlyGrouped.toStream().to("q1-monthly", Produced.with(
                  WindowedSerdes.timeWindowedSerdeFrom(Long.class, Long.MAX_VALUE), CustomSerdes.ValQ1()));

        weeklyGrouped.toStream().to("q1-weekly", Produced.with(
                  WindowedSerdes.timeWindowedSerdeFrom(Long.class, Long.MAX_VALUE), CustomSerdes.ValQ1()));

        hourlyGrouped.toStream().to("q1-hourly", Produced.with(
                WindowedSerdes.timeWindowedSerdeFrom(Long.class, Long.MAX_VALUE), CustomSerdes.ValQ1()));

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp(); //clean up of the local StateStore
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
