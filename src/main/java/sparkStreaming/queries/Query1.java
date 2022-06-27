package sparkStreaming.queries;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import utils.Event;
import utils.tuples.ValQ1;

import java.sql.Timestamp;

public class Query1 extends Query {

    JavaStreamingContext streamingContext;
    JavaInputDStream<ConsumerRecord<String, Event>> src;

    public Query1(JavaStreamingContext streamingContext, JavaInputDStream<ConsumerRecord<String, Event>> src) {
        this.src = src;
        this.streamingContext = streamingContext;
    }

    @Override
    public void execute() throws Exception {
        String outputPath = "q1-res";

        var dataStream = src
                .filter(record -> record.value() != null && record.value().getSensor_id() < 10000)
                .mapToPair(record -> {
                    ValQ1 valQ1 = new ValQ1();
                    valQ1.setOccurrences(1L);
                    valQ1.setTemperature(record.value().getTemperature());
                    valQ1.setSensor_id(record.value().getSensor_id());
                    valQ1.setTimestamp(record.value().getTimestamp()); //settare poi timestamp in base alla finestra
                    return new Tuple2<>(record.value().getSensor_id(), valQ1);
                });

        var grouped = dataStream.reduceByKeyAndWindow(
                (Function2<ValQ1, ValQ1, ValQ1>) (val1, val2) -> {
            ValQ1 result = new ValQ1();
            result.setTemperature(val1.getTemperature() + val2.getTemperature());
            result.setOccurrences(val1.getOccurrences() + val2.getOccurrences());
            result.setSensor_id(val1.getSensor_id());
            result.setTimestamp(val1.getTimestamp());
            return result;
        },
                (Function2<ValQ1, ValQ1, ValQ1>) (val1, val2) -> {
                    ValQ1 res = new ValQ1();
                    res.setTemperature(val1.getTemperature() - val2.getTemperature());
                    res.setOccurrences(val1.getOccurrences() + val2.getOccurrences());
                    res.setSensor_id(val1.getSensor_id());
                    res.setTimestamp(val1.getTimestamp());
                    return res;
                }, Durations.seconds(30), Durations.seconds(30));

        var results = grouped.map(value -> {
            ValQ1 res = value._2();
            double meanTemperature = res.getTemperature() / (double)res.getOccurrences();
            res.setTemperature(meanTemperature);
            res.setTimestamp(value._2().getTimestamp());
            return res;
        });

        results.print();
        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
