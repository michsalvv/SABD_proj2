package queries.flink;

import flink.Event;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.impl.CsvEncoder;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.kafka.clients.producer.ProducerRecord;
import queries.flink.aggregate.Average;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import queries.Query;
import utils.CSVEncoder;
import utils.OutputQuery;
import utils.ValQ1;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;
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
                .window(TumblingEventTimeWindows.of(Time.minutes(60)))
                .allowedLateness(Time.minutes(2))                                           // funziona
                .aggregate(new Average())
                .setParallelism(4);

        dataStream.print();

        OutputFileConfig config = OutputFileConfig.builder()
                .withPartSuffix(".csv")
                .build();

        final StreamingFileSink<OutputQuery> sink = StreamingFileSink
                .forRowFormat(new Path(outputPath), new CSVEncoder())
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
        SE CI CHIEDE DI SCRIVE SU KAFKA IL RISULTATO STA GIA FATTO QUA NON CANCELLARE T'AMMAZZO
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
