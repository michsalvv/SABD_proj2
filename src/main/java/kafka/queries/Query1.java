package kafka.queries;

import javassist.bytecode.stackmap.TypeData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import utils.Event;
import utils.serdes.CustomSerdes;
import utils.serdes.EventSerializer;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

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
    public void execute() throws Exception {
        String outputPath = "q1-res";

        var dataStream = src
                .filter((key, event) -> event!=null && event.getSensor_id() < 10000)
                //.groupBy((key, event) -> event.getSensor_id())
                .selectKey((key, event) -> event.getSensor_id())
                .groupByKey(Grouped.with(Serdes.Long(), CustomSerdes.Event()));
                //.reduce((event, v1) -> {
                //    Double temp = event.getTemperature() + v1.getTemperature();
                //    v1.setTemperature(temp);
                //    return v1;
                //});

        dataStream.count().toStream().print(Printed.toSysOut());
        //dataStream.toStream().to("output", Produced.with(Serdes.Long(), CustomSerdes.Event()));

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp(); //clean up of the local StateStore
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
