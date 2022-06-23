package flink.queries.aggregate;

import java.sql.Timestamp;

public class AccumulatorQ1 {
    long count;
    double sum;
    long sensor_id;
    Timestamp last_timestamp;
}