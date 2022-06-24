package flink.queries;

import flink.deserialize.Event;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import flink.queries.aggregate.AvgQ1;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import utils.tuples.ValQ1;

import java.util.concurrent.TimeUnit;

public class Query1 extends Query {
    StreamExecutionEnvironment env;
    DataStreamSource<Event> src;

    public Query1(StreamExecutionEnvironment env, DataStreamSource<Event> src) {
        this.env = env;
        this.src = src;
    }

    @Override
    public void execute() throws Exception {
        String outputPath = "q1-res";

        var dataStream = src
                .filter(event -> event.getSensor_id() < 10000)
                .setParallelism(2)
                .keyBy(Event::getSensor_id)
                .window(TumblingEventTimeWindows.of(Time.minutes(10)))
                .allowedLateness(Time.minutes(2))                                           // funziona
                .aggregate(new AvgQ1())
                .setParallelism(4);

        dataStream.print();

        OutputFileConfig config = OutputFileConfig.builder()
                .withPartSuffix(".csv")
                .build();

        final StreamingFileSink<ValQ1> sink = StreamingFileSink
                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<ValQ1>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMinutes(2))
                                .withInactivityInterval(TimeUnit.MINUTES.toMinutes(1))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .withOutputFileConfig(config)
                .build();
        dataStream.addSink(sink);               // Il sink deve avere parallelismo 1
    /*
        Properties prop = new Properties();
        prop.setProperty("transaction.timeout.ms", "900000");

        KafkaSink kafkaSink = KafkaSink.builder()
                .setBootstrapServers("kafka-broker:9092")
                .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("top")
                .setRecordSerializer(new KafkaRecordSerializationSchema<Object>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(Object element, KafkaSinkContext context, Long timestamp) {
                        return new ProducerRecord<>("testTopic", element.toString().getBytes(StandardCharsets.UTF_8));
                    }
                })
                .setKafkaProducerConfig(prop)
                .build();

        dataStream.sinkTo(kafkaSink);

     */
        env.execute("Query 1");
    }
}