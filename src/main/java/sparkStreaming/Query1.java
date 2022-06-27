package sparkStreaming;

import flink.deserialize.Event;
import flink.deserialize.EventDeserializer;
import flink.queries.Query;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.LocationStrategy;
import scala.Tuple2;
import scala.Tuple3;
import utils.tuples.ValQ1;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class Query1 extends Query {

    @Override
    public void execute() throws Exception {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("JavaNetworkWordCount");

        JavaStreamingContext jSC = new JavaStreamingContext(sparkConf, Durations.seconds(1));
        Collection<String> topics = Arrays.asList("flink-events");

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "kafka-broker:29092");
        kafkaParams.put("key.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class);
        kafkaParams.put("value.deserializer", EventDeserializer.class);
        kafkaParams.put("group.id", "id" + new Timestamp(System.currentTimeMillis()).toString());
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaParams.put("enable.auto.commit", false);

        var stream = KafkaUtils.createDirectStream(jSC, LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, Event>Subscribe(topics, kafkaParams))
                .filter(record ->
                        {
                            System.out.println(record);
                            return record.value() != null && record.value().getSensor_id() < 100000;
                        })
                .mapToPair(record -> {
                    ValQ1 valQ1 = new ValQ1();
                    valQ1.setOccurrences(1l);
                    valQ1.setTemperature(record.value().getTemperature());
                    valQ1.setSensor_id(record.value().getSensor_id());
                    valQ1.setTimestamp(record.value().getTimestamp());
                    return new Tuple2<>(record.value().getSensor_id(), valQ1);
                });


        var grouped = stream
                .reduceByKeyAndWindow((Function2<ValQ1, ValQ1, ValQ1>) (val_1, val_2) -> {
                    ValQ1 result = new ValQ1();
                    result.setTemperature(val_1.getTemperature() + val_2.getTemperature());
                    result.setOccurrences(val_1.getOccurrences() + val_2.getOccurrences());
                    result.setSensor_id(val_1.getSensor_id());
                    result.setTimestamp(val_2.getTimestamp());
                    return result;
                }, Duration.apply(1000*30));

        var results = grouped.map(value -> {
            ValQ1 res = value._2;
            double meanTemperature = res.getTemperature() / (double) res.getOccurrences();
            res.setTemperature(meanTemperature);
            res.setTimestamp(value._2().getTimestamp(), "SECONDS");
            System.out.println(res);
            return res;
        });

//        results.print();
        results.foreachRDD(rdd -> rdd.saveAsTextFile("./sparkStreamingResults"));

//        results.print(20);
        jSC.start();
//        jSC.stop();
        jSC.awaitTermination();

    }
}
