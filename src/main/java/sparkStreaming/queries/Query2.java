package sparkStreaming.queries;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import utils.Event;
import utils.tuples.ValQ1;

public class Query2 extends Query {

    JavaStreamingContext streamingContext;
    JavaInputDStream<ConsumerRecord<String, Event>> src;

    public Query2(JavaStreamingContext streamingContext, JavaInputDStream<ConsumerRecord<String, Event>> src) {
        this.src = src;
        this.streamingContext = streamingContext;
    }

    @Override
    public void execute() throws Exception {
        String outputPath = "q2-res";

        var dataStream = src
                .filter(record -> record.value() != null && record.value().getSensor_id() < 10000)
                .mapToPair(record -> {
                    ValQ1 valQ1 = new ValQ1();
                    valQ1.setOccurrences(1L);
                    valQ1.setTemperature(record.value().getTemperature());
                    valQ1.setSensor_id(record.value().getSensor_id());
                    return new Tuple2<>(record.value().getSensor_id(), valQ1);
                });

        var grouped = dataStream.reduceByKeyAndWindow((Function2<ValQ1, ValQ1, ValQ1>)
                (v1, v2) -> {
            ValQ1 result = new ValQ1();
            result.setTemperature(v1.getTemperature() + v2.getTemperature());
            result.setOccurrences(v1.getOccurrences() + v2.getOccurrences());
            result.setSensor_id(v1.getSensor_id());
            return result;
                }, Durations.seconds(10));

        var results = grouped.map(value -> {
            ValQ1 res = value._2();
            res.setTemperature(res.getTemperature() / (double) res.getOccurrences());
            return res;
        });

        results.print();
        streamingContext.start();
        streamingContext.stop();
    }
}
