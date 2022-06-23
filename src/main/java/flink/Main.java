package flink;

import flink.deserialize.EventDeserializer;
import flink.deserialize.Event;
import flink.queries.Query;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import flink.queries.Query1;
import flink.queries.Query2;
import flink.queries.Query3;

public class Main {
    static Query query;
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
                .setValueOnlyDeserializer(new EventDeserializer())
                .build();

        // BIBBIA
        var src = env.fromSource(source, WatermarkStrategy
                .<Event>forMonotonousTimestamps()
                .withTimestampAssigner((event, l) -> event.getTimestamp().getTime()),
                "Kafka Source");

        Query q1 = new Query1(env,src);
        Query q2 = new Query2(env, src);
        Query q3 = new Query3(env, src);

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
            case ("Q1S"):
//                query=q1SQL;
                break;
            case ("Q2S"):
//                query=q2SQL;
                break;
        }
        query.execute();
    }
}
