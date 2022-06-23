package flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import queries.flink.Query1;
import queries.flink.Query2;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class Main {
    //TODO Fare un serializzatore vero
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // start a checkpoint every 30s
        env.enableCheckpointing(30000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(15000);

        // checkpoints have to complete within one minute, or are discarded
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // only two consecutive checkpoint failures are tolerated
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        // allow only one checkpoint to be in progress at the same time
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // sets the checkpoint storage where checkpoint snapshots will be written
        env.getCheckpointConfig().setCheckpointStorage("file:///opt/flink/flink-checkpoints");
        // enable checkpointing with finished tasks
        Configuration config = new Configuration();
        config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);

        env.configure(config);

        KafkaSource<Event> source = KafkaSource.<Event>builder()
                .setBootstrapServers("kafka-broker:9092")
                .setTopics("flink-events")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
//                .setUnbounded(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new CustomDeserializer())
                .build();

        // BIBBIA
        var src = env.fromSource(source, WatermarkStrategy
                .<Event>forMonotonousTimestamps()
                .withTimestampAssigner((event, l) -> event.getTimestamp().getTime()),
                "Kafka Source");

        var q1 = new Query1(env,src);
//        var q2 = new Query2(env, src);
        q1.execute();
//        q2.execute();

    }
}
