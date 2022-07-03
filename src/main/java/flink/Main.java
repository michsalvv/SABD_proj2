package flink;

import utils.serdes.EventSerde;
import utils.tuples.Event;
import flink.queries.Query;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import flink.queries.Query1;
import flink.queries.Query2;
import flink.queries.Query3;
import utils.Config;

public class Main {
    static Query query;
    public static void main(String[] args) throws Exception {
        int parallelism = Integer.parseInt(args[1]);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(3000);      // Checkpoint every 30 seconds
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);

        env.getCheckpointConfig().setCheckpointTimeout(60000);            // Checkpoint have to complete in 1 minute
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3); // only two consecutive checkpoint failures are tolerated
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);         // allow only one checkpoint to be in progress at the same time
        env.getCheckpointConfig().setCheckpointStorage("file:///opt/flink/flink-checkpoints");

        env.setParallelism(parallelism);                                 // Set parallelismo choosed by user

        env.getConfig().setLatencyTrackingInterval(1000);

        Configuration config = new Configuration();
        config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);      // enable checkpointing with finished tasks
        env.configure(config);

        KafkaSource<Event> source = KafkaSource.<Event>builder()
                .setBootstrapServers("kafka-broker:9092")
                .setTopics("flink-events")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new EventSerde())
                .build();

        // Watermark Strategy
        var src = env.fromSource(source, WatermarkStrategy
                .<Event>forMonotonousTimestamps(),"Kafka Source")
                .setParallelism(1);


        Query q1 = new Query1(env,src);
        Query q2 = new Query2(env,src);
        Query q3 = new Query3(env,src);

        switch (args[0]) {
            case ("Q1"):
                query = q1;
                break;
            case ("Q2"):
                query=q2;
                break;
            case ("Q3"):
                query=q3;
                break;
        }
        query.execute();
    }
}
