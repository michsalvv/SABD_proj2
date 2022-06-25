package sparkStreaming;

import flink.deserialize.Event;
import flink.deserialize.EventDeserializer;
import flink.queries.Query;
import flink.queries.Query2;
import flink.queries.Query3;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Main {
    static Query query;
    public static void main(String[] args) throws Exception {

        Query1 query = new Query1();
        query.execute();
    }
}
